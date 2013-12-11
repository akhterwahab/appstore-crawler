import logging
import ConfigParser
from dataPipeline import index_builder
from dataPipeline.framework.sequential_parallel_framework import SequentialParallelFramework 

class DataConsumer(SequentialParallelFramework.Consumer):
    def __init__(self, conf_path):
	SequentialParallelFramework.Consumer.__init__(self)
	self.logger_ = logging.getLogger("DataConsumer")
	self.cfg_ = ConfigParser.ConfigParser()
	self.cfg_.read(conf_path)
	#index config
	self.index_key_ = self.cfg_.get("index", "key")
	self.logger_.debug("read config '[index]key':" + self.index_key_)
	index_conf_path = self.cfg_.get("index", "conf_path")
	self.logger_.debug("read config '[index]conf_path':" + index_conf_path)
	self.index_builder_ = index_builder.IndexBuilder(index_conf_path) 

    def consume_begin(self):
	pass

    def consume_end(self):
	self.index_builder_.build()
	

    def consume(self, element):
	object_id = str(element["creative"][self.index_key_])
	for key in element["extension"].keys():
	    for index_key in tuple(element["extension"][key]):
	        self.index_builder_.add(index_key, object_id)
	     
	    
	
	
	
	

	
