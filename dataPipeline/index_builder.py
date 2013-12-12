import os
import time
import shutil
import logging
import ConfigParser
from dataPipeline import index_merger


class IndexBuilder(object):
    def __init__(self, conf_path):
        self.conf_path_ = conf_path 
	self.logger_ = logging.getLogger("IndexBuilder")
        self.cfg_ = ConfigParser.ConfigParser()
        self.cfg_.read(conf_path)
        self.index_data_dir_ = self.cfg_.get("index", "data_dir")
	self.logger_.debug("read config '[index]data_dir':" + self.index_data_dir_)
        self.index_tmp_data_dir_ = self.cfg_.get("index", "tmp_dir")
	self.logger_.debug("read config '[index]tmp_dir':" + self.index_tmp_data_dir_)
        self.index_tmp_filepaths_ = []
        self.index_filename_id_ = 0
        self.index_date_pattern_ = self.cfg_.get("index", "date_pattern") or "%Y%m%d"
	self.logger_.debug("read config '[index]date_pattern':" + self.index_date_pattern_)
        self.index_name_prefix_ = self.cfg_.get("index", "name_prefix")
	self.logger_.debug("read config '[index]name_prefix':" + self.index_name_prefix_)
        self.max_index_count_ = int(self.cfg_.get("index", "max_index_count_per_file")) or 100000
	self.logger_.debug("read config '[index]max_index_count_per_file':" + str(self.max_index_count_))
	self.index_filename_ = self.cfg_.get("index", "index_filename")
	self.logger_.debug("read config '[index]index_filename':" + str(self.index_filename_))
	self.index_merger_ = index_merger.IndexMerger(self.index_tmp_data_dir_, True)
        self.__reset()

    def build(self):
        self.__flush()
        filepath = self.index_merger_.merge(self.index_tmp_filepaths_)
        data_datetime = time.strftime(self.index_date_pattern_, time.localtime(time.time()))  
        target_filepath = "%s/%s-%s" % (self.index_data_dir_, self.index_name_prefix_, data_datetime)
        shutil.move(filepath, target_filepath)
	self.index_tmp_filepaths = []
 
	tmp_index_path = "%s/.%s.index.tmp" % (self.index_data_dir_, self.index_name_prefix_)
	f = open(tmp_index_path, "w")
	f.write("%s\n" % target_filepath)
	f.close()
	target_indexfile_path = "%s/%s" % (self.index_data_dir_, self.index_filename_)
	shutil.move(tmp_index_path, target_indexfile_path) 
             
    def __get_index_filename(self): 
        return "%s.tmp-%d" % (self.index_name_prefix_, \
                            self.index_filename_id_) 
           
    def __reset(self):
        self.index_ = { "count" : 0, "index" : dict() }

    def __flush(self):
        if self.index_["count"] > 0:
            filename = self.__get_index_filename()
            filepath = "%s/%s" % (self.index_tmp_data_dir_, filename)
            fp = open(filepath, "w")
            for key in sorted(self.index_["index"].keys()):
		record =  "%s\t%s\t%s\n" % (key, self.index_["index"][key][0], "\t".join(sorted(self.index_["index"][key][1])))
		fp.write(record.encode('utf8'))
            fp.close()
            self.index_filename_id_ += 1
            self.index_tmp_filepaths_.append(filepath)

    def add(self, key, value):
        if not self.index_["index"].has_key(key):
            self.index_["index"][key] = [0, set()]
        #if value in index[key]
        if not value in self.index_["index"][key][1]:
            self.index_["index"][key][0] += 1
            self.index_["index"][key][1].add(value)
            self.index_["count"] += 1

        if self.index_["count"] >= self.max_index_count_:
            self.__flush()
            self.__reset()
