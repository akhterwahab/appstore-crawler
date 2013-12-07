from dataPipeline import event_analyzer
from dataPipeline import indexer
from dataPipeline.framework.sequential_parallel_framework import SequentialParallelFramework 

class DataConsumer(SequentialParallelFramework.Consumer):
    def __init__(self, conf_path):
	SequentialParallelFramework.Processor.__init__(self)
	self.cfg_ = ConfigParser.ConfigParser()
	self.cfg_.read(conf_path)
	#redis config
        redis_host = self.cfg_.get("redis", "host") or "127.0.0.1"
        redis_port = int(self.cfg_.get("redis", "port")) or 6379
        redis_db = int(self.cfg_.get("redis", "db")) or 0
        redis_max_connection = int(self.cfg_.get("redis", "max_connection"))
        self.redis_client_ = redis_client.RedisClient(redis_host, \
                         redis_port, redis_db, redis_max_connection)

	#index config
	self.index_key_ = self.cfg_.get("index", "key")
	index_data_output = self.cfg_.get("index", "output_path")
	self.indexer_ = indexer.Indexer(index_data_output) 

	
    def consume(self, element):
	(extension_event, data_object) = element 
	if extension_event == event_analyzer.EVENT.ADD
	    or extension_event == event_analyzer.EVENT.UPDATE:
	    object_id = str(element["creative"][self.index_key_])
	    for key in element["extension"].keys():
		for index_key in tuple(element["extension"][key]):
		    self.indexer_.add(index_key, object_id)
	elif extension_event == event_analyzer.EVENT.IGNORE:
	    pass
	    
	     
	    
	
	
	
	

	
