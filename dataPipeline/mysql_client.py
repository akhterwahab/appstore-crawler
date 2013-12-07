import MySQLdb
from DBUtils.PooledDB import PooledDB

class MySQLClient(object):
    def __init__(self, host, port, db, \
			user, passwd, max_connection, charset):
	'''
        self.cfg_ = ConfigParser.ConfigParser()
	self.cfg_.read(conf_path)
	self.host_ = self.cfg_.get("mysql", "host") or "127.0.0.1"
	self.port_ = int(self.cfg_.get("mysql", "port")) or 3306
	self.db_ = self.cfg_.get("mysql", "db")
	self.user_ = self.cfg_.get("mysql", "user")
	self.passwd_ = self.cfg_.get("mysql", "passwd")
	self.max_connection_ = int(self.cfg_.get("mysql", "conneciton_")) or 1
	self.charset_ = self.cfg_.get("mysql", "charset") or "utf8"
	'''
	self.host_ = host
	self.port_ = port
	self.db_ = db
	self.user_ = user
	self.passwd_ = passwd
	self.max_connection_ = max_connection
	self.charset_ = charset
	self.connect()
	

    def connect(self):
	self.db_client_ = PooledDB(MySQLdb, maxusage=self.max_connection_, \
					db=self.db_, host=self.host_, \
					user=self.user_, passwd=self.passwd_, \
					charset=self.charset_)

    def __del__(self):
	self.db_client_.close()

    def get_connection(self):
	if not self.db_client_:
	    self.connect()
	return self.db_client_.connection() 


