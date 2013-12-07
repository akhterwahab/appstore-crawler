
import hashlib
import urllib
import redis_client
import ConfigParser

class Event(object):
    ADD = 0
    UPDATE = 1
    DELETE = 2
    IGNORE = 3


class EventAnalyzer(object): 
    def __init__(self, conf_path):
        self.cfg_ = ConfigParser.ConfigParser()
        self.cfg_.read(conf_path)
        self.sign_fields_ = set(self.cfg_.get("sign", "fields").split(","))
        self.sign_key_ = self.cfg_.get("sign", "key")
        redis_host = self.cfg_.get("redis", "host") or "127.0.0.1"
        redis_port = int(self.cfg_.get("redis", "port")) or 6379
        redis_db = int(self.cfg_.get("redis", "db")) or 0
        redis_max_connection = int(self.cfg_.get("redis", "max_connection"))
        self.redis_client_ = redis_client.RedisClient(redis_host, \
                         redis_port, redis_db, redis_max_connection)

    '''
    use config file "pipeline.sign_fields" check data_object is duplicate,
    input data_object should be a dict 
    return True if all selected sign_fields's value is the same,
    else return False
    '''

    def get_event(self, data_object):
        def validate_type():
            if not isinstance(data_object, dict):
                raise DataProcessor.InvalidTypeException("data_object invalid type, need type<%s> but %s." \
                                % ("dict", type(data_object)))
        validate_type()
        sign_value = self.get_sign(data_object)
        redis_instance = self.redis_client_.get_connection()
        sign_key = str(data_object[self.sign_key_])
        redis_value = redis_instance.get(sign_key)
        if redis_value is None:
            return Event.ADD
        elif redis_value == sign_value:
            return Event.IGNORE
        else:
            return Event.UPDATE

    def get_sign(self, data_object):
        sign_content = ""
        for key in sorted(self.sign_fields_):
            if data_object.has_key(key):
                sign_content += "%s=%s&" % (str(key), urllib.quote(str(data_object[key])))
            else:
                sign_content += "%s=&" % (str(key))
        sign_value = hashlib.new("md5", sign_content).digest()
        return sign_value 

     

            
            


