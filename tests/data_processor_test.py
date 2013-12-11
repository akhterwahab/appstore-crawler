import unittest
import os
if __name__ == "__main__":
    import sys
    sys.path.append("..")
from dataPipeline import data_producer

class dataProcessorTest(unittest.TestCase): 
    def setUp(self):
	fp = open("data_processor_test.conf", "w")
	fp.write("[configpath]\ncreative = creative.conf\nextension = extension.conf\nmysql = mysql.conf\n" \
		 "[sql]\ninsert_creative = insert into creative(id, );\ninsert_value_key = \n" \
		 "update_creative = update creative set id = \n;" \
		 "update_value_key = \n")
	fp.close()
	
	fp = open("mysql.conf", "w")
	fp.write("[mysql]\nhost = 127.0.0.1\nport = 3306\ndb = appstore\nuser = root\n" \
			"passwd = \nmax_connection = 10\ncharset = utf8\n")
	fp.close()

	fp = open("creative.conf", "w")
        fp.write("[sign]\nkey = key\nfields = key,required_value,repeated_value\n" \
		"[redis]\nhost = 127.0.0.1\nport = 6379\ndb = 0\nmax_connection = 10\n")
	fp.close()

	fp = open("extension.conf", "w")
        fp.write("[sign]\nkey = key\nfields = key,required_value,repeated_value\n" \
		"[redis]\nhost = 127.0.0.1\nport = 6379\ndb = 0\nmax_connection = 10\n")
	fp.close()


    def tearDown(self):
	os.remove("itunes.index")
	os.remove("itunesNewFreeApplication.dat")
	os.remove("data_producer_test.conf")
		
		
    def testcase_processor(self):
	processor = data_processor.DataProcessor("data_processor_test.conf")
	for element in self.elements:
	    e = processor.process()
	    self.assertTrue(element == e)

     

if __name__ == "__main__":
    unittest.main()

