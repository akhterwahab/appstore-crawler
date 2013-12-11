import json
import urllib
import hashlib
import base64
import logging
import ConfigParser
import MySQLdb
from dataPipeline.framework.sequential_parallel_framework import SequentialParallelFramework 
from dataPipeline import mysql_client 
from dataPipeline import redis_client

class DataProcessor(SequentialParallelFramework.Processor):
    class InvalidTypeException(Exception): pass
    class Event(object):
        ADD = 0
        UPDATE = 1
        DELETE = 2
        IGNORE = 3


    def __init__(self, conf_path):
        SequentialParallelFramework.Processor.__init__(self)
	self.logger_ = logging.getLogger("DataProcessor")
	self.cfg_ = ConfigParser.ConfigParser()
	self.cfg_.read(conf_path)

	#read creative config 
	self.creative_insert_sql_ = self.cfg_.get("sql", "insert_creative")
	self.logger_.debug("read config '[sql]insert_creative':" + self.creative_insert_sql_)
	self.creative_insert_key_ = self.cfg_.get("sql", "insert_key").split(",")
	self.logger_.debug("read config '[sql]insert_key':" + str(self.creative_insert_key_))
	self.creative_update_sql_ = self.cfg_.get("sql", "update_creative")
	self.logger_.debug("read config '[sql]update_creative':" + self.creative_update_sql_)
	self.creative_update_key_ = self.cfg_.get("sql", "update_key").split(",")
	self.logger_.debug("read config '[sql]update_key':" + str(self.creative_insert_key_))

	#read mysql config
	mysql_host = self.cfg_.get("mysql", "host") or "127.0.0.1"
	self.logger_.debug("read config '[mysql]host':" + mysql_host)
	mysql_port = int(self.cfg_.get("mysql", "port")) or 3306
	self.logger_.debug("read config '[mysql]port':" + str(mysql_port))
	mysql_db = self.cfg_.get("mysql", "db")
	self.logger_.debug("read config '[mysql]db':" + mysql_db)
	mysql_user = self.cfg_.get("mysql", "user")
	self.logger_.debug("read config '[mysql]user':" + mysql_user)
	mysql_passwd = self.cfg_.get("mysql", "passwd")
	self.logger_.debug("read config '[mysql]passwd':" + mysql_passwd)
	mysql_max_connection = int(self.cfg_.get("mysql", "max_connection")) or 1
	self.logger_.debug("read config '[mysql]max_connection':" + str(mysql_max_connection))
	mysql_charset = self.cfg_.get("mysql", "charset") or "utf8"
	self.logger_.debug("read config '[mysql]charset':" + mysql_charset) 
	self.mysql_client_ = mysql_client.MySQLClient(host=mysql_host, port=mysql_port, \
			db=mysql_db, user=mysql_user, passwd=mysql_passwd, \
			max_connection = mysql_max_connection, charset = mysql_charset)

	#read sign config
	self.sign_fields_ = set(self.cfg_.get("sign", "fields").split(","))
	self.logger_.debug("read config '[sign]fields':" + str(self.sign_fields_))
        self.sign_key_ = self.cfg_.get("sign", "key")
	self.logger_.debug("read config '[sign]key':" + self.sign_key_)
        #read redis config
        redis_host = self.cfg_.get("redis", "host") or "127.0.0.1"
	self.logger_.debug("read config '[redis]host':" + redis_host)
        redis_port = int(self.cfg_.get("redis", "port")) or 6379
	self.logger_.debug("read config '[redis]port':" + str(redis_port))
        redis_db = int(self.cfg_.get("redis", "db")) or 0
	self.logger_.debug("read config '[redis]db':" + str(redis_db))
        redis_max_connection = int(self.cfg_.get("redis", "max_connection"))
	self.logger_.debug("read config '[redis]max_connection':" + str(redis_max_connection))
        self.redis_client_ = redis_client.RedisClient(redis_host, \
                         redis_port, redis_db, redis_max_connection)

    def __insert_creative(self, creative):
	db_connection = self.mysql_client_.get_connection()
	db_cursor = db_connection.cursor()
	sql = self.creative_insert_sql_ % tuple(["%s" % creative.get(key) for key in self.creative_insert_key_]) 
	self.logger_.debug("insert sql: " + sql)
	try:
	    db_cursor.execute(sql)
	    db_connection.commit()
	except Exception, e:
	    db_connection.rollback()
	    self.logger_.fatal(str(e))
	db_connection.close()

    def __update_creative(self, creative):
	db_connection = self.mysql_client_.get_connection()
	db_cursor = db_connection.cursor()
	sql = self.creative_update_sql_ % tuple(["%s" % creative.get(key) for key in self.creative_update_key_]) 
	self.logger_.debug("update sql: " + sql)
	try:
	    db_cursor.execute(sql)
	    db_connection.commit()
	except Exception, e:
	    db_connection.rollback()
	    self.logger_.fatal(str(e))
	db_connection.close()

    def process_begin(self):
	pass
	
    def process_end(self):
	pass

    def process(self, element):
        data_object = json.loads(element)
	self.__save_creative(data_object["creative"])
	return data_object

    def __save_creative(self, creative_object):
        sign_key = str(creative_object[self.sign_key_])
	sign_value = self.__get_sign(creative_object)

        redis_instance = self.redis_client_.get_connection()
	redis_value = redis_instance.get(sign_key)

        if redis_value is None:
            creative_event = DataProcessor.Event.ADD
        elif redis_value == sign_value:
            creative_event = DataProcessor.Event.IGNORE
        else:
            creative_event = DataProcessor.Event.UPDATE

	self.logger_.debug("creative event: " + str(creative_event))

	if creative_event == DataProcessor.Event.ADD:
	    self.__insert_creative(creative_object)
	elif creative_event == DataProcessor.Event.UPDATE:
	    self.__update_creative(creative_object)
	redis_instance.set(sign_key, sign_value)

    def __get_sign(self, data_object):
        sign_content = ""
        for key in sorted(self.sign_fields_):
            if data_object.has_key(key):
                sign_content += "%s=%s&" % (key, data_object[key])
            else:
                sign_content += "%s=&" % (key)
        sign_value = hashlib.new("md5", sign_content.encode("utf8")).digest()
        return sign_value 


