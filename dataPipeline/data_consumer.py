import logging
import time
import os
import shutil
import ConfigParser
from dataPipeline import index_builder
from dataPipeline import index_merger
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
	self.new_index_filename_ = self.cfg_.get("index", "new_filename")
	self.logger_.debug("read config '[index]new_filename':" + self.new_index_filename_)
	self.merge_index_filename_ = self.cfg_.get("index", "merge_filename")
	self.logger_.debug("read config '[index]merge_filename':" + self.merge_index_filename_)
	self.tmp_dir_ = self.cfg_.get("index", "tmp_dir")
	self.logger_.debug("read config '[index]tmp_dir':" + self.tmp_dir_)
	self.merge_data_dir_ = self.cfg_.get("index", "merge_dir")
	self.logger_.debug("read config '[index]merge_dir':" + self.merge_data_dir_)
	self.new_data_dir_ = self.cfg_.get("index", "new_dir")
	self.logger_.debug("read config '[index]new_dir':" + self.new_data_dir_)
	self.index_date_pattern_ = self.cfg_.get("index", "date_pattern") or "%Y%m%d"
	self.logger_.debug("read config '[index]date_pattern':" + self.index_date_pattern_)
        self.merge_name_prefix_ = self.cfg_.get("index", "merge_prefix")
	self.logger_.debug("read config '[index]merge_prefix':" + self.merge_name_prefix_)




	self.index_builder_ = index_builder.IndexBuilder(index_conf_path) 
	self.index_merger_ = index_merger.IndexMerger(self.tmp_dir_, False)

    def consume_begin(self):
	pass

    def consume_end(self):
	self.index_builder_.build()
	merge_index_filepath = "%s/%s" % (self.merge_data_dir_, self.merge_index_filename_)
	new_index_filepath = "%s/%s" % (self.new_data_dir_, self.new_index_filename_)

	merge_filepaths = []
	if os.path.exists(merge_index_filepath):
	    f = open(merge_index_filepath)
	    merge_index_data_filepath = f.readline().replace("\n", "")
	    f.close()
	    merge_filepaths.append(merge_index_data_filepath)
	if os.path.exists(new_index_filepath):
	    f = open(new_index_filepath)
	    new_index_data_filepath = f.readline().replace("\n", "")
	    f.close()
	    merge_filepaths.append(new_index_data_filepath)

	filepath = self.index_merger_.merge(merge_filepaths)
	if filepath:
            data_datetime = time.strftime(self.index_date_pattern_, time.localtime(time.time()))  
            target_filepath = "%s/%s-%s" % (self.merge_data_dir_, self.merge_name_prefix_, data_datetime)
            shutil.move(filepath, target_filepath)
	    tmp_index_path = "%s/.%s.index.tmp" % (self.tmp_dir_, self.merge_name_prefix_)
	    f = open(tmp_index_path, "w")
	    f.write("%s\n" % target_filepath)
	    f.close()
	    target_mergefile_path = "%s/%s" % (self.merge_data_dir_, self.merge_index_filename_)
	    shutil.move(tmp_index_path, target_mergefile_path) 

    def consume(self, element):
	object_id = str(element["creative"][self.index_key_])
	for key in element["extension"].keys():
	    for index_key in tuple(element["extension"][key]):
	        self.index_builder_.add(index_key, object_id)
	     
	    
	
	
	
	

	
