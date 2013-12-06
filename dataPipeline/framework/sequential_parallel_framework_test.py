# -*- coding: utf8 -*-
from sequential_parallel_framework import SequentialParallelFramework
import logging
import time

class EchoProducer(SequentialParallelFramework.Producer):
    def __init__(self, count, sleep_time):
        self.logger_ = logging.getLogger("EchoProducer")
        self.count_ = count
	self.sleep_time_ = sleep_time

    def produce(self):
        element = None
	time.sleep(1.0 * self.sleep_time_ / 1000)
        if self.count_ != 0:
            element = "%d\t2\t2011-08-25 08:17:21\t2012-03-07 08:05:18\t1\tVOGSO大作战\t\\N\tcustom\t\\N\t\t\\N\t\\N\tall\t111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111\tyes\tyes\t0\t1\t100\t4be292d81d41c82fd50000d3\tpercent\t0\t0\t\\N\t\t0\t1\t\\N\t\\N\t1\tapplist\tnone\tnone\tyes\t0\t1\t\\N\t1\t0\t3\txp\t\\N\tall\t100\t3\t\\N\t\\N\t\\N\t0\t\\N\t1\t0\t1\t\\N" % self.count_
            #self.logger_.info("Producer produce element: %d" % self.count_)
            self.count_ -= 1
        return element

class EchoProcessor(SequentialParallelFramework.Processor):
    def __init__(self, sleep_time):
        self.logger_ = logging.getLogger("EchoProcessor")
	self.sleep_time_ = sleep_time

    def process(self, element):
        elements =  element.split("\t")
	time.sleep(1.0 * self.sleep_time_ / 1000)
        #self.logger_.info("Processor process element: %s" % elements[0])
        return elements

class EchoConsumer(SequentialParallelFramework.Consumer):
    def __init__(self, sleep_time):
        self.logger_ = logging.getLogger("EchoConsumer")
	self.sleep_time_ = sleep_time

    def consume(self, element):
        self.logger_.info("Consumer consumer element: %s" % element[0])
	time.sleep(1.0 * self.sleep_time_ / 1000)




def process_slow():
    pd_sleep_time = 1
    pc_sleep_time = 10
    cn_sleep_time = 1
    producer = EchoProducer(1201, pd_sleep_time)
    processor = EchoProcessor(pc_sleep_time)
    consumer = EchoConsumer(cn_sleep_time)
    thread_size = 10
    queue_size = 17
    timeout = 1
    framework = SequentialParallelFramework(producer, processor, consumer, thread_size, queue_size, timeout)
    framework.run()
    framework.join()

if __name__ == '__main__':
    import os
    logging.basicConfig(filename = os.path.join(os.getcwd(), 'framework.log'), level = logging.INFO)
    process_slow()

