import redis        

class Monitor:
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connection = None

    def __del__(self):
        try:
            self.reset()
        except:
            pass

    def reset(self):
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def monitor(self):
        if self.connection is None:
            self.connection = self.connection_pool.get_connection(
                'monitor', None)
        self.connection.send_command("monitor")
        return self.listen()

    def parse_response(self):
        msg = self.connection.read_response()
        return 

    def listen(self):
        while True:
            yield self.parse_response()





class Monitor:
    def __init__(self, r):
        self.r = r

    def run(self):
        STATE = "waiting"
        with r.monitor() as m:
            for command in m.listen():
                if STATE == "waiting":
                    pass
                if STATE == "waiting":
                    pass


if  __name__ == '__main__':
    pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    # monitor = Monitor(pool)
    # commands = monitor.monitor()

    # for c in commands:
    #     print(c)

    r = redis.Redis(host='localhost', port=6379, db=0)
    with r.monitor() as m:
        for command in m.listen():
            print({k: v[:50] if isinstance(v, (str, bytes)) else v for k, v in command.items()})
