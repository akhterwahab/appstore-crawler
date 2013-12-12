import unittest
import json
import os
if __name__ == "__main__":
    import sys
    sys.path.append("..")
from dataPipeline import data_processor

class dataProcessorTest(unittest.TestCase): 
    def setUp(self):
	fp = open("data_processor_test.conf", "w")
	fp.write("[configpath]\ncreative = creative.conf\nextension = extension.conf\nmysql = mysql.conf\n" \
		 "[sql]\ninsert_creative = replace into creative values(%s, 0, 0, NOW(), \"%s\", 0, 0, 0, 0, " \
		 "\"standard\", NULL, \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"175x175\"," \
		 "\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", 0, NULL, 0);\n" \
		 "insert_key = app_store_id,name,app_name,package_name,package_size,app_version," \
		 "app_store_id,icon,url,ad_pic,title,ad_desc,ad_desc_brief,provider,screenshot\n" \
		 "update_creative = replace into creative values(%s, 0, 0, NOW(), \"%s\", 0, 0, 0, 0, " \
		 "\"standard\", NULL, \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"175x175\"," \
		 "\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", 0, NULL, 0);\n" \
		 "update_key = app_store_id,name,app_name,package_name,package_size,app_version\n")
	fp.write("[mysql]\nhost = 127.0.0.1\nport = 3306\ndb = appstore\nuser = root\n" \
			"passwd = \nmax_connection = 10\ncharset = utf8\n")
        fp.write("[sign]\nkey = app_store_id\nfields = ad_desc_brief,app_name,package_name," \
		 "title,url,ad_pic,package_size,ad_desc,app_store_id,provider,icon,app_version,screenshots,name\n"
		"[redis]\nhost = 127.0.0.1\nport = 6379\ndb = 0\nmax_connection = 10\n")
	fp.close()
	fp = open("data_processor_test.dat", "w")
	for ix in xrange(0, 100):
	    fp.write('{"extension": {"supportedDevices": ["iPad2Wifi", "iPadWifi", "iPad3G", "iPadMini4G", "iPadMini", "iPadFourthGen4G", "iPadThirdGen", "iPadFourthGen", "iPadThirdGen4G", "iPad23G"], "terms": ["\u7f8e\u98df\u4f73\u996e", "\u5065\u5eb7\u5065\u7f8e"], "features": []}, "creative": {"app_version": "1.0", "ad_desc_brief": "Die beliebten Obst- und Gem\u00fcsebrosch\u00fcren der AMA jetzt als \u00fcbersichtliche App. ", "package_name": "at.ama.kiosk", "title": "AMA Obst/Gem\u00fcse-Welt", "url": "https://itunes.apple.com/cn/app/ama-obst-gemuse-welt/id775733674?mt=8&uo=4", "ad_pic": "http://a138.phobos.apple.com/us/r30/Purple/v4/10/e9/8a/10e98a95-4e35-20c7-d26f-6871928db24c/mzl.bmgvlhfe.png", "package_size": "62746407", "ad_desc": "Die beliebten Obst- und Gem\u00fcsebrosch\u00fcren der AMA jetzt als \u00fcbersichtliche App. \\n\\nGeordnet nach Kategorien - mit vielen interessanten Informationen und raffinierten Rezepten. Auch gedruckt erh\u00e4ltlich im AMA Webshop.", "app_name": "AMA Obst/Gem\u00fcse-Welt", "app_store_id": "775733674", "provider": "blockhaus entertainment", "icon": "http://a528.phobos.apple.com/us/r30/Purple/v4/4b/e4/46/4be44659-29d6-a02c-46de-8976bffea1ee/AppIcon72x72.png", "screenshots": "", "name": "AMA Obst/Gem\u00fcse-Welt"}}\n')
	fp.close


    def tearDown(self):
	os.remove("data_processor_test.dat")
	os.remove("data_processor_test.conf")
		
		
    def testcase_processor(self):
	processor = data_processor.DataProcessor("data_processor_test.conf")
	fp = open("data_process_test.dat")
	for element in fp:
	    e = processor.process(element)
	    self.assertTrue(json.loads(element) == e)

     

if __name__ == "__main__":
    unittest.main()

