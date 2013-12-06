import redis

class RedisClient(object):
    def __init__(self, host='localhost', port=6379, db=0, max_connections=1):
        self.host_ = host
        self.port_ = port
        self.db_ = db
        self.max_connections_ = max_connections
        self.pool_ = redis.ConnectionPool(max_connections = max_connections, \
                                        host=host, port=port, db=db)

    def get_instance(self):
        return redis.Redis(connection_pool=self.pool_)
