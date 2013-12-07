import logging
import ConfigParser
from dataPipeline.framework.sequential_parallel_framework import SequentialParallelFramework 
from dataPipeline import event_analyzer

class DataProducer(SequentialParallelFramework.Producer):
    def __init__(self, conf_path):
        self.logger_ = logging.getLogger("dataProducer")
        #find index file path
	self.cfg_ = ConfigParser.ConfigParser()
	self.cfg_.read(conf_path)
        self.data_dir_ = self.cfg_.get("data", "data_dir")
        self.index_filename_ = self.cfg_.get("data", "index_filename")
        self.index_filepath_ = "%s/%s" % (data_dir, index_path)
        #read index file and find data file path
        self.index_fp_ = open(self.index_filepath_)
        self.data_filename_ = self.index_fp.readline()
        self.index_fp_.close()
        self.data_filepath_ = "%s/%s" % (data_dir, self.data_filename_)
        #open data path
        self.data_fp_ = open(self.data_filepath_)

    def produce(self):
        return self.data_fp_.readline()
