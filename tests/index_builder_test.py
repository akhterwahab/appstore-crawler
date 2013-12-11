import unittest
import time
import os
if __name__ == "__main__":
    import sys
    sys.path.append("..")
from dataPipeline import index_builder
from dataPipeline import index_reader

class indexerBuilderTest(unittest.TestCase): 
    def setUp(self):
	fp = open("index_builder_test.conf", "w")
	fp.write("[index]\ndata_dir = ../data\ntmp_dir = /tmp\n" \
		 "date_pattern = %Y%m%d\nname_prefix = index\nmax_index_count_per_file = 4\n")
	fp.close()

    def tearDown(self): 
	os.remove("index_builder_test.conf")
	
	
    def testcase_build(self):
        indexer = index_builder.IndexBuilder("index_builder_test.conf")
	checksum = {}
        for ix in xrange(0, 317):
            kix = ix % 73
	    key = "key%d" % kix
	    value = "value%d" % ix
            indexer.add(key, value)
	    if not checksum.has_key(key):
		checksum[key] = set()
	    checksum[key].add(value)
        indexer.build()
	data_datetime = time.strftime(indexer.index_date_pattern_, time.localtime(time.time()))
        target_filepath = "%s/%s-%s" % (indexer.index_data_dir_, indexer.index_name_prefix_, data_datetime)
	reader = index_reader.IndexFileReader(target_filepath)
	
	(key, count, values) = reader.read()
	while not key is None:
	    self.assertTrue(checksum.has_key(key))
	    for value in values:
	        self.assertTrue(value in checksum[key])
		checksum[key].remove(value)
	    self.assertTrue(len(checksum[key]) == 0)
	    checksum.pop(key)
	    (key, count, values) = reader.read()
	self.assertTrue(len(checksum) == 0)
	os.remove(target_filepath)

if __name__ == "__main__":
    unittest.main()

