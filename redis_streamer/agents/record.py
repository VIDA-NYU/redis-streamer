# Class to record all desired redis activity

import base64
import os
import re
import time
import json
import tqdm
import codecs
import redis

from mcap.writer import Writer
from redis.client import Monitor as Monitor_

from redis_streamer import Agent, ctx, utils


class Monitor(Monitor_):
    monitor_re = re.compile(rb"\[(\d+) (.*?)\] (.*)")
    command_re = re.compile(rb'"(.*?(?<!\\))"')
    def next_command(self):
        """Parse the response from a monitor command"""
        # https://github.com/redis/redis/blob/4031a187321be26fc96c044fec3ee759841e8379/src/replication.c#L576
        response = self.connection.read_response(disable_decoding=True)
        command_time, command_data = response.split(b" ", 1)
        db_id, client_info, raw_command = self.monitor_re.match(command_data).groups()
        command_parts = self.command_re.findall(raw_command)
        command_parts = [codecs.escape_decode(x)[0] for x in command_parts]

        client_info = client_info.decode()
        if client_info == "lua":
            client_address = client_type = "lua"
            client_port = ""
        else:
            client_address, client_port = client_info.rsplit(":", 1)  # ipv6 has colons
            client_type = "unix" if client_info.startswith("unix") else "tcp"
        return {
            "time": float(command_time.decode()),
            "db": int(db_id.decode()),
            "client_address": client_address,
            "client_port": client_port,
            "client_type": client_type,
            # "raw": response,
            # "command": command,
            "args": command_parts,
        }



class Recorder:
    def __init__(self, schema, out_dir='.'):
        self.out_dir = out_dir
        self.writer = self.fhandle = None
        self.schema = schema
        self.channel_ids = {}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def open_writer(self, record_name, force=False):
        if self.writer is not None and force:
            self.close_writer()

        if self.writer is None:
            # create new writer
            os.makedirs(self.out_dir or '.', exist_ok=True)
            self.fname = fname = os.path.join(self.out_dir, f'{record_name}.mcap')
            self.fhandle = fhandle = open(fname, "wb")
            self.writer = writer = Writer(fhandle)
            writer.start()
            print("Opening writer", fname)

            # https://mcap.dev/docs/python/mcap-apidoc/mcap.well_known#mcap.well_known.MessageEncoding
            self.schema_id = writer.register_schema(
                name="data",
                encoding="jsonschema",
                data=json.dumps({
                    "type": "object",
                    "properties": self.schema,
                }).encode(),
            )

    def open_channel(self, channel='all'):
        if channel not in self.channel_ids:
            self.channel_ids[channel] = self.writer.register_channel(
                schema_id=self.schema_id,
                topic=channel,
                message_encoding="json",
            )

    def write(self, timestamp, channel='all', **data):
        self.writer.add_message(
            channel_id=self.channel_ids[channel],
            log_time=time.time_ns(),
            data=json.dumps(data).encode("utf-8"),
            publish_time=int(timestamp * 10e9),
        )

    def close(self):
        if self.writer is not None:
            print("closing", self.fname)
            self.writer.finish()
            self.fhandle.close()
            self.writer = self.fhandle = None
            self.channel_ids.clear()



def record_monitor(record_key='RECORD:NAME', record_cmds=('set', 'xadd')):
    '''Record all commands using MONITOR command.'''
    pbar = tqdm.tqdm()

    with Recorder(schema={ "cmd": { "type": "array" } }) as rec, \
         redis.Redis(host='localhost', port=6379, db=0, decode_responses=False) as r:
        with Monitor(r.connection_pool) as m:
            # look up the initial recording name
            record_name = (r.get(record_key) or b'').decode()
            print("recording name:", record_name)

            for data in m.listen():
                # parse command
                cmd_items = data['args']
                if not cmd_items:  # rare parsing problems
                    continue
                cmd_name = cmd_items[0].decode().lower()
                if cmd_name not in {'multi', 'exec'}:
                    pbar.set_description(cmd_name)

                # listen for delete key
                if cmd_name == 'del':
                    for key in cmd_items[1:]:
                        if key == record_key:
                            rec.close()
                            record_name = None
                            print("ended recording")
                            continue

                # listen for set key
                if cmd_name == 'set':
                    key, value = cmd_items[1:3]
                    if key == record_key and record_name != value:
                        rec.close()
                        record_name = value
                        print("new recording:" if record_name else "ended recording", record_name)
                        continue
                
                # no recording
                if not record_name:
                    continue
                
                # make sure a writer is open
                rec.open_writer(record_name)
                if cmd_name not in record_cmds:
                    continue

                # pick which channel to write to
                if cmd_name in {'set', 'xadd'}:
                    sid = b':'.join(cmd_items[:2]).decode()
                else:
                    sid = cmd_items[0].decode()
                rec.open_channel(sid)

                # write command to file
                cmd_items = [base64.b64encode(x).decode() for x in cmd_items]
                rec.write(data['time'], sid, cmd=cmd_items)
                pbar.update()


async def record_streams(stream_ids='*', record_key='XRECORD:NAME', stream_refresh=3):
    '''An alternative recording implementation that explicitly reads from streams using XREAD.'''
    pbar = tqdm.tqdm()
    all_streams = stream_ids == '*'
    if all_streams:
        og_cursor = {}
    else:
        stream_ids = stream_ids.split('+') + [record_key]
        og_cursor = {s: '$' for s in stream_ids}

    await ctx.init()
    with Recorder(schema={ "d": { "type": "string" } }) as rec:
        record_name = None
        agent = Agent()

        cursor = dict(og_cursor)
        rec_cursor = {record_key: '-'}
        t0s = time.time()
        while True:
            # FIXME: this doesn't wait for all data up to this time to be written after a recording ends. 
            # check for recording changes
            results, rec_cursor = await agent.read(rec_cursor, latest=True, count=1, block=2)
            for sid, xs in results:
                # for t, x in xs:
                #     tqdm.tqdm.write(f'{sid} {t} {x}')
                if sid == record_key:
                    for t, x in xs:
                        x = (x.get(b'd') or b'').decode() or None
                        pbar.set_description(f'{sid} {t} {x!r} {record_name!r}')
                        if record_name != x:
                            rec.close()
                            record_name = x
                            cursor = dict(og_cursor)
                            print("new recording:" if record_name else "ended recording", record_name)
                            continue
            
            if not record_name:
                continue

            # keep up-to-date list of streams
            t1s = time.time()
            if not cursor or (all_streams and t1s - t0s > stream_refresh):
                keys = {x.decode('utf-8') async for x in ctx.r.scan_iter(match=stream_ids or None, _type='stream')}
                for k in keys - set(cursor):
                    cursor[k] = '$'
                t0s = t1s

            # read data from redis and write to file
            results, cursor = await agent.read(cursor, latest=False, count=1, block=5000)
            for sid, xs in results:
                rec.open_writer(record_name, sid)
                for t, x in xs:
                    pbar.set_description(f'{sid} {t}')
                    rec.write(utils.parse_epoch_time(t), sid, **{k.decode(): base64.b64encode(v).decode() for k, v in x.items()})
                    pbar.update()



def watch():
    with redis.Redis(host='localhost', port=6379, db=0) as r:
        with Monitor(r.connection_pool) as m:
            for data in m.listen():
                cmd_items = data['args']
                print(*[x[:30] for x in cmd_items], max(len(x) for x in cmd_items))


def read(fname):
    from mcap.reader import make_reader
    with open(fname, "rb") as f:
        reader = make_reader(f)
        print(reader.get_header())
        # summary = reader.get_summary()
        # print(summary)
        for schema, channel, message in reader.iter_messages():
            print(f"{channel.topic} ({schema.name}): {message.data}")


if __name__ == '__main__':
    import fire
    fire.Fire({
        "monitor": record_monitor,
        "streams": record_streams,
        'watch': watch,
        "read": read,
    })