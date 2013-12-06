from framework.sequential_parallel_framework import SequentialParallelFramework 
import logging

class DataProducer(SequentialParallelFramework.Producer):
    def __init__(self, data_dir, index_filename):
        self.logger_ = logging.getLogger("dataProducer")
        #find index file path
        self.data_dir_ = data_dir
        self.index_filename_ = index_filename
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
