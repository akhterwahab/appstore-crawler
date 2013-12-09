import unittest
if __name__ == "__main__":
    import sys
    sys.path.append("..")
from dataPipeline import mysql_client

class mysqlClientTest(unittest.TestCase): 
    def testcase_insert_and_select(self):
	mc = mysql_client.MySQLClient("127.0.0.1", 3306, "appstore", \
			"root", "", 1, 'utf8')   
	
	id = 1
	name = "insert and select"
	price = 0.7

	insert_sql = "insert into unittest values(%d, '%s', %f)" \
			% (id, name, price) 
	select_sql = "select * from unittest where id = %d" % id
        delete_sql = "delete from unittest where id = %d" % id
	conn = mc.get_connection()
	cursor = conn.cursor()
	cursor.execute(insert_sql)
	conn.commit()
	cursor.execute(select_sql)
	result = cursor.fetchall()
	self.assertTrue(len(result) == 1)
	self.assertTrue(result[0] == (id, name, price))
	cursor.execute(delete_sql)
	conn.commit()

    def testcase_update_and_select(self):
	mc = mysql_client.MySQLClient("127.0.0.1", 3306, "appstore", \
			"root", "", 1, 'utf8')   
	
	id = 1
	name = "update and select"
	price = 0.7
	change_price = 1.3

	insert_sql = "insert into unittest values(%d, '%s', %f)" \
			% (id, name, price) 
	update_sql = "update unittest set price = %f where id = %d" \
			% (change_price, id)
	select_sql = "select * from unittest where id = %d" % id
        delete_sql = "delete from unittest where id = %d" % id
	conn = mc.get_connection()
	cursor = conn.cursor()
	cursor.execute(insert_sql)
	conn.commit()
	cursor.execute(update_sql)
	conn.commit()
	cursor.execute(select_sql)
	result = cursor.fetchall()
	self.assertTrue(len(result) == 1)
	self.assertTrue(result[0] == (id, name, change_price))
	cursor.execute(delete_sql)
	conn.commit()



if __name__ == "__main__":
    unittest.main()

