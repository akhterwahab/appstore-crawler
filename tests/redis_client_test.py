import threading
import time
import unittest

if __name__ == "__main__":
    import sys
    sys.path.append("..")

from dataPipeline import redis_client

class RedisThread(threading.Thread):
    def __init__(self, thread_id, redis_client):
        threading.Thread.__init__(self)
        self.redis_client_ = redis_client
        self.thread_id_ = thread_id
    
    def run(self):
        ri = self.redis_client_.get_connection()
        ri.set("multithread_key_%d" % self.thread_id_, \
               "multithread_value_%d" % self.thread_id_)
        
        assert("multithread_value_%d" % self.thread_id_ \
                        == ri.get("multithread_key_%d" % self.thread_id_))
        #time.sleep(5)
 

class redisClientTest(unittest.TestCase):

    def testcase_get_and_set(self):
        rc = redis_client.RedisClient("127.0.0.1", 6379, 0, 10)
        ri = rc.get_connection()
        ri.set("key1", "value1")
        self.assertTrue("value1" == ri.get("key1"))
    
    def testcase_many_client(self):
        rc = redis_client.RedisClient("127.0.0.1", 6379, 0, 10)
        ris = [rc.get_connection() for ix in xrange(0, 15)]
        for ix in xrange(0, len(ris)):
            ri = ris[ix]
            ri.set("many_key_%d" % ix, "many_value_%d" % ix)
            self.assertTrue("many_value_%d" % ix == ri.get("many_key_%d" % ix))
    
   
    def testcase_multithread_client(self):
        rc = redis_client.RedisClient("127.0.0.1", 6379, 0, 10)
        redis_threads = [RedisThread(ix, rc) for ix in xrange(0, 15)]
        for redis_thread in redis_threads: 
            redis_thread.start()
        for redis_thread in redis_threads: 
            redis_thread.join()
    
    
if __name__ == "__main__":
    unittest.main()
    
