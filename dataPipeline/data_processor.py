import json
import ConfigParser
import MySQLdb
from dataPipeline.framework.sequential_parallel_framework import SequentialParallelFramework 
from dataPipeline import event_analyzer
from dataPipeline import mysql_client 

class DataProcessor(SequentialParallelFramework.Processor):
    class InvalidTypeException(Exception): pass

    def __init__(self, conf_path):
        SequentialParallelFramework.Processor.__init__(self)
	self.cfg_ = ConfigParser.ConfigParser()
	self.cfg_.read(conf_path)

	#read creative config 
	creative_conf_path = self.cfg_.get("configfile", "creative")
	self.creative_insert_sql_ = self.cfg_.get("sql", "insert_creative")
	self.creative_insert_key_ = self.cfg_.get("sql", "insert_value_key").split(",")
	self.creative_update_sql_ = self.cfg_.get("sql", "update_creative")
	self.creative_update_key_ = self.cfg_.get("sql", "update_valute_key").split(",")
	self.creative_event_analyzer_ = event_analyzer.EventAnalyzer(creative_conf_path)

	#read extension config 
	extension_conf_path = self.cfg_.get("configfile", "extension")
        self.extension_event_analyzer_ = event_analyzer.EventAnalyzer(extension_conf_path)

	#read mysql config
	mysql_conf_path = self.cfg_.get("configfile", "mysql")
        mysql_cfg_ = ConfigParser.ConfigParser()
	mysql_cfg_.read(mysql_conf_path)
	host = mysql_cfg_.get("mysql", "host") or "127.0.0.1"
	port = int(mysql_cfg_.get("mysql", "port")) or 3306
	db = mysql_cfg_.get("mysql", "db")
	user = mysql_cfg_.get("mysql", "user")
	passwd = mysql_cfg_.get("mysql", "passwd")
	max_connection = int(mysql_cfg_.get("mysql", "conneciton_")) or 1
	charset = mysql_cfg_.get("mysql", "charset") or "utf8"

	self.mysql_client_ = mysql_client.MySQLClient(host=host, port=port, \
			db=db, user=user, passwd=passwd, \
			max_conneciton = max_connection), charset = charset)

    def insert_creative(self, creative):
	db_connection = self.mysql_client_.get_connection()
	db_cursor = db_connection.cursor()
	sql = self.creative_insert_sql_ % tuple([creative[key] for key in self.creative_insert_key_) 
	db_cursor.execute(sql)
	db_conneciton.close()

    def update_creative(self, creative):
	db_connection = self.mysql_client_.get_connection()
	db_cursor = db_connection.cursor()
	sql = self.creative_update_sql_ % tuple([creative[key] for key in self.creative_update_key_) 
	db_cursor.execute(sql)
	db_conneciton.close()

	
    def process(self, element):
        jo = json.loads(element)
        (creative_event, extension_event) = self.analyze_event(jo)
	if creative_event == event_analyzer.Event.ADD:
	    insert_creative(jo["creative"])
	elif creative_event == event_analyzer.Event.UPDATE:
	    update_creative(jo["creative"])
	return (extension_event, jo)

    def analyze_event(self, data_object):
	if data_object.has_key("creative") and data_object.has_key("extension"):
            creative_event = self.creative_event_analyzer.get_event(data_object["creative"])
	    extension_event = self.extension_event_analyzer.get_event(data_object["extension"])
	return (creative_event, extension_event)

   
        

 
        
