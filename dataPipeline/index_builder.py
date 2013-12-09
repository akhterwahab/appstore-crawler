import os
import time
import shutil
import ConfigParser


class IndexBuilder(object):

    class IndexFileReader(object):
	def __init__(self, filepath):
	    self.filepath_ = filepath
	    self.fp_ = open(filepath)

        def __del__(self):
	    self.fp_.close()
	
	def read(self):
	    line = self.fp_.readline()
	    if line:
	        record = line.replace("\n", "").split("\t")
	    else:
	        record = None
	    if record:
	        return (record[0], int(record[1]), set(record[2:]))
	    else:
		return (None, 0, [])

	     
    def __init__(self, conf_path):
        self.conf_path_ = conf_path 
        self.cfg_ = ConfigParser.ConfigParser()
        self.cfg_.read(conf_path)
        self.index_data_dir_ = self.cfg_.get("index", "data_dir")
        self.index_tmp_data_dir_ = self.cfg_.get("index", "tmp_dir")
        self.index_tmp_filepaths_ = []
        self.index_filename_id_ = 0
        self.index_date_pattern_ = self.cfg_.get("index", "date_pattern") or "%Y%m%d"
        self.index_name_prefix_ = self.cfg_.get("index", "name_prefix")
        self.max_index_count_ = int(self.cfg_.get("index", "max_index_count_per_file")) or 100000
        self.__reset()

    def build(self):
        self.__flush()
        filepath = self.__merge_sort(self.index_tmp_filepaths_)
        data_datetime = time.strftime(self.index_date_pattern_, time.localtime(time.time()))  
        target_filepath = "%s/%s-%s" % (self.index_data_dir_, self.index_name_prefix_, data_datetime)
        shutil.move(filepath, target_filepath)

    def __merge_sort(self, filepaths):
        if len(filepaths) > 1:
            low = 0
            high = len(filepaths)
            mid = (low + high) / 2
            filepaths = (self.__merge_sort(filepaths[low:mid]), self.__merge_sort(filepaths[mid:high]))
        return self.__merge(filepaths)

    def __merge(self, filepaths):
        if len(filepaths) == 0:
            return ""
        elif len(filepaths) == 1:
            return filepaths[0]
        else:
            filename = self.__get_index_filename()
            filepath = "%s/%s" % (self.index_tmp_data_dir_, filename)
            fp = open(filepath, "w") 
            left_filepath = filepaths[0] 
            right_filepath = filepaths[1]
            left_reader = IndexBuilder.IndexFileReader(left_filepath)
            right_reader = IndexBuilder.IndexFileReader(right_filepath)
            (l_key, l_valuec, l_values) = left_reader.read()
            (r_key, r_valuec, r_values) = right_reader.read()
            while True:
                if l_key and r_key:
                    if l_key < r_key:
                        fp.write("%s\t%d\t%s\n" % (l_key, l_valuec, "\t".join(l_values)))
                        (l_key, l_valuec, l_values) = left_reader.read()
                    elif l_key > r_key:
                        fp.write("%s\t%d\t%s\n" % (r_key, r_valuec, "\t".join(r_values)))
                        (r_key, r_valuec, r_values) = right_reader.read()
                    else:
                        union_values = l_values.union(r_values)
                        fp.write("%s\t%d\t%s\n" % (r_key, len(union_values), "\t".join(sorted(union_values))))
                        (l_key, l_valuec, l_values) = left_reader.read()
                        (r_key, r_valuec, r_values) = right_reader.read()
                elif not l_key and not r_key:
                    break
                elif l_key and not r_key:
                    fp.write("%s\t%d\t%s\n" % (l_key, l_valuec, "\t".join(l_values)))
                    (l_key, l_valuec, l_values) = left_reader.read()
                elif r_key and not l_key:
                    fp.write("%s\t%d\t%s\n" % (r_key, r_valuec, "\t".join(r_values)))
                    (r_key, r_valuec, r_values) = right_reader.read()
            fp.flush()
            fp.close()
	    self.index_filename_id_ += 1
            os.remove(left_filepath)
            os.remove(right_filepath)
            return filepath
             
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
                fp.write("%s\t%d\t%s\n" % (str(key), self.index_["index"][key][0], \
                            "\t".join(sorted(self.index_["index"][key][1]))))
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
