import logging
import ConfigParser
from dataPipeline.framework.sequential_parallel_framework import SequentialParallelFramework 

class DataProducer(SequentialParallelFramework.Producer):
    def __init__(self, conf_path):
        self.logger_ = logging.getLogger("dataProducer")
        #find index file path
	self.logger_.debug("read config file: " + conf_path)
	self.cfg_ = ConfigParser.ConfigParser()
	self.cfg_.read(conf_path)
        self.data_dir_ = self.cfg_.get("data", "data_dir")
	self.logger_.debug("read config '[data]data_dir': " + self.data_dir_)

        self.index_filename_ = self.cfg_.get("data", "index_filename")
	self.logger_.debug("read config '[data]index_filename': " + self.index_filename_)

        self.index_filepath_ = "%s/%s" % (self.data_dir_, self.index_filename_)
        #read index file and find data file path
        self.index_fp_ = open(self.index_filepath_)
        self.data_filename_ = self.index_fp_.readline().replace("\n", "")
	self.logger_.debug("read index file: '" + self.index_filepath_ \
			+ "', data_filename: '" + self.data_filename_ + "'")
        self.index_fp_.close()
        self.data_filepath_ = "%s/%s" % (self.data_dir_, self.data_filename_)
        #open data path
	self.logger_.debug("open data file: " + self.data_filepath_)
        self.data_fp_ = open(self.data_filepath_)

    def produce_begin(self):
	pass
 
    def produce_end(self):
	pass

    def produce(self):
        element = self.data_fp_.readline()
	#self.logger_.debug("produce element: " + str(element))
	return element
