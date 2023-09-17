import redis

if  __name__ == '__main__':
    r = redis.Redis(host='localhost', port=6379, db=0)
    with r.monitor() as m:
        for command in m.listen():
            print({k: v[:50] if isinstance(v, (str, bytes)) else v for k, v in command.items()})