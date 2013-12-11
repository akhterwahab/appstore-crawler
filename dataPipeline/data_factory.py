import logging
import sys
sys.path.append(".")
import ConfigParser
from dataPipeline import data_producer
from dataPipeline import data_processor
from dataPipeline import data_consumer
from dataPipeline.framework import sequential_parallel_framework


if __name__ == '__main__':
    import os
    #logging.basicConfig(filename = os.path.join(os.getcwd(), 'framework.log'), level = logging.INFO)
    logging.basicConfig(filename = os.path.join(os.getcwd(), 'log/framework.log'), level = logging.DEBUG)

    conf_path = "conf/itunes.conf"
    cfg = ConfigParser.ConfigParser()
    cfg.read(conf_path)
    thread_size = cfg.get("framework", "thread_size") or 8
    queue_size = cfg.get("framework", "queue_size") or 20
    timeout = cfg.get("framework", "timeout") or 1
 
    producer = data_producer.DataProducer(conf_path)
    processor = data_processor.DataProcessor(conf_path) 
    consumer = data_consumer.DataConsumer(conf_path)
    framework = sequential_parallel_framework.SequentialParallelFramework(producer, processor, consumer, \
					int(thread_size), int(queue_size), int(timeout))
    framework.run()
    framework.join()
