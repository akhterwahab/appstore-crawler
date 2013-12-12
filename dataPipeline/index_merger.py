import os
import logging
import shutil
import sys
if __name__ == '__main__':
    sys.path.append(".")
from dataPipeline import index_reader

class IndexMerger(object):
    def __init__(self, tmp_data_dir, auto_delete):
	self.logger_ = logging.getLogger("IndexMerger")
        self.merge_id_ = 0
        self.index_tmp_data_dir_ = tmp_data_dir
	self.auto_delete_ = auto_delete
        
    def merge(self, filepaths):
        return self.__merge_sort(filepaths)

    def __get_next_merge_filename(self): 
        filename = ".index-%d.merge" % (self.merge_id_) 
        self.merge_id_ += 1
        return filename

    def __merge_sort(self, filepaths):
        if len(filepaths) > 1:
            low = 0
            high = len(filepaths)
            mid = (low + high) / 2
            filepaths = (self.__merge_sort(filepaths[low:mid]), self.__merge_sort(filepaths[mid:high]))
        filepaths =  self.__merge(filepaths)
        return filepaths

    def __merge(self, filepaths):
        if len(filepaths) == 0:
            return ""
        elif len(filepaths) == 1:
            return filepaths[0]
        else:
            filename = self.__get_next_merge_filename()
            filepath = "%s/%s" % (self.index_tmp_data_dir_, filename)
            fp = open(filepath, "w") 
            left_filepath = filepaths[0] 
            right_filepath = filepaths[1]
            left_reader = index_reader.IndexFileReader(left_filepath)
            right_reader = index_reader.IndexFileReader(right_filepath)
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
            left_reader.close()
            right_reader.close()
            fp.flush()
            fp.close()
	    if self.auto_delete_:
                os.remove(left_filepath)
                os.remove(right_filepath)

            return filepath

if __name__ == '__main__':
    if len(sys.argv) <= 3:
	print "usage: "
    else:
	tmp_path = sys.argv[1]
	auto_delete = bool(sys.argv[2] == 'True') or False
	desc_path = sys.argv[3]
	filepaths = sys.argv[4:]
        merger = IndexMerger(tmp_path, auto_delete)
	filepath = merger.merge(filepaths)
	if filepath:
	    shutil.move(filepath, desc_path)
	

        
    

