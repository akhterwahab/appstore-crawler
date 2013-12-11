import unittest
import os
if __name__ == "__main__":
    import sys
    sys.path.append("..")
from dataPipeline import data_producer

class dataConsumerTest(unittest.TestCase): 
    def setUp(self):
	self.elements = ["itunes\n", "scrapy\n", "python\n"]
	fp = open("itunesNewFreeApplication.dat", "w")
	for element in self.elements:
	    fp.write(element)
	fp.close()
	fp = open("itunes.index", "w")
	fp.write("itunesNewFreeApplication.dat")
	fp.close()
	fp = open("data_producer_test.conf", "w")
	fp.write("[data]\ndata_dir = .\nindex_filename = itunes.index\n")
	fp.close()
	
    def tearDown(self):
	os.remove("itunes.index")
	os.remove("itunesNewFreeApplication.dat")
	os.remove("data_producer_test.conf")
		
		
    def testcase_produce(self):
	producer = data_producer.DataConsumer("data_producer_test.conf")
	for element in self.elements:
	    e = producer.produce()
	    self.assertTrue(element == e)
if __name__ == "__main__":
    unittest.main()

