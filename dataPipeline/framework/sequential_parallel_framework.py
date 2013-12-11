import logging
import threading
import multiprocessing
import traceback

def execute():
    global producer_worker_
    return producer_worker_.producer_.produce()

class SequentialParallelFramework(object):
    class UmimplementedException(Exception): pass
    class InvalidTypeException(Exception): pass
    class InvalidArgumentException(Exception): pass

    def run(self):
	self.producer_.produce_begin()
	self.processor_.process_begin()
	self.consumer_.consume_begin()

        self.producer_worker_.start()
        for ix in xrange(0, self.thread_size_):
            self.processor_workers_[ix].start()
        self.consumer_worker_.start()

    def join(self):
        try:
            self.producer_worker_.join()
            for ix in xrange(0, self.thread_size_):
                self.processor_workers_[ix].join()
            self.consumer_worker_.join()
	    self.producer_.produce_end()
	    self.processor_.process_end()
	    self.consumer_.consume_end()
        except Exception, e:
            self.logger_.fatal("SequentialParallelFramework join encounter a exception: %s, "\
				"traceback: %s" % (str(e), traceback.format_exc()))

    def __init__(self, producer, processor, \
                    consumer, thread_size, queue_size, \
                    timeout):
        def validate_argument():
            if producer is None:
                raise SequentialParallelFramework.InvalidArgumentException("producer is None")
            if processor is None:
                raise SequentialParallelFramework.InvalidArgumentException("processor is None")
            if consumer is None:
                raise SequentialParallelFramework.InvalidArgumentException("consumer is None")
            if thread_size < 0:
                raise SequentialParallelFramework.InvalidArgumentException("thread_size is less than 0")
            if queue_size < 0:
                raise SequentialParallelFramework.InvalidArgumentException("queue_size is less than 0")
            if timeout < 0:
                raise SequentialParallelFramework.InvalidArgumentException("timeout is less than 0")

        def validate_type():
            if not isinstance(producer, SequentialParallelFramework.Producer):
                raise ParallelFramework.InvalidTypeException("producer invalid type, need class<%s> but %s." \
                        % ("SequentialParallelFramework.Producer", type(producer)))

            if not isinstance(processor, SequentialParallelFramework.Processor):
                raise ParallelFramework.InvalidTypeException("processor invalid type, need class<%s> but %s." \
                        % ("SequentialParallelFramework.Processor", type(processor)))

            if not isinstance(consumer, SequentialParallelFramework.Consumer):
                raise ParallelFramework.InvalidTypeException("consumer invalid type, need class<%s> but %s." \
                        % ("SequentialParallelFramework.Consumer", type(consumer)))

        validate_argument()
        validate_type()

	self.producer_ = producer
	self.processor_ = processor
	self.consumer_ = consumer

        self.logger_ = logging.getLogger("SequentialParallelFramework")
        self.thread_size_ = thread_size
        self.queue_size_ = queue_size
        self.create_running_state()
        self.create_element_queues()
        self.create_semaphore()
        self.create_thread(producer, processor, consumer, timeout)
        self.logger_.info("SequentialParallelFramework ready")

    def create_running_state(self):
        self.is_producer_running_ = True
        self.is_processor_running_ = [True for ix in xrange(0, self.thread_size_)]
        self.is_consumer_running_ = True
        self.is_framework_running_ = True

    def create_element_queues(self):
        self.producer_element_queues_ = [SequentialParallelFramework.CircleQueue(self.queue_size_) for ix in xrange(0, self.thread_size_)]
        self.consumer_element_queues_ = [SequentialParallelFramework.CircleQueue(self.queue_size_) for ix in xrange(0, self.thread_size_)]

    def create_semaphore(self):
        self.queue_empty_sems_ = [threading.Semaphore(1) for ix in xrange(0, self.thread_size_)]
        self.queue_full_sems_ = [threading.Semaphore(0) for ix in xrange(0, self.thread_size_)]
        self.queue_ready_sems_ = [threading.Semaphore(0) for ix in xrange(0, self.thread_size_)]

    def create_thread(self, producer, processor, consumer, timeout):
        self.producer_worker_ = SequentialParallelFramework.ProducerWorker(self, producer, timeout)
        self.processor_workers_ = [SequentialParallelFramework.ProcessorWorker(self, processor, ix) for ix in xrange(0, self.thread_size_)]
        self.consumer_worker_ = SequentialParallelFramework.ConsumerWorker(self, consumer)

    class ProducerWorker(threading.Thread):
        def __init__(self, framework, producer, timeout):
            threading.Thread.__init__(self)
            self.framework_ = framework
            self.producer_ = producer
            self.timeout_ = timeout
            self.logger_ = logging.getLogger("SequentialParallelFramework.ProducerWorker")
            #self.executor_ = SequentialParallelFramework.Pool(processes=1)
            self.executor_ = multiprocessing.Pool(processes=1, \
                                initializer = SequentialParallelFramework.ProducerWorker.initializer, \
                                initargs = (self,))

        def initializer(producer_worker):
            global producer_worker_
            producer_worker_ = producer_worker

        def run(self):
            queue_id = 0
            try:
                self.logger_.info("ProducerWorker thread start")
                while self.framework_.is_framework_running_ \
                        and self.framework_.is_producer_running_:
                   self.logger_.debug("ProducerWorker wait for queue[%d] EMTPY semaphore" % queue_id)
                   self.framework_.queue_empty_sems_[queue_id].acquire()
                   self.logger_.debug("ProducerWorker acquire queue[%d] EMTPY semaphore" % queue_id)
                   while not self.framework_.producer_element_queues_[queue_id].isfull():
                       try:
                           future = self.executor_.apply_async(execute)
                           element = future.get(self.timeout_)
                           if not element:
                               self.framework_.is_producer_running_ = False
                               break
                           else:
                               self.framework_.producer_element_queues_[queue_id].enqueue(element)
                       except multiprocessing.TimeoutError:
                           if not self.framework_.producer_element_queues_[queue_id].isempty():
                               self.logger_.warn("Producer produce nothing is %d s, " \
                                                "but the queue is not emtpy, " \
                                                "switch to a new queue", self.timeout_)
                               break
                           else:
                               self.logger_.warn("Producer produce nothing in %d s, " \
                                                 "but the queue is empty, wait an element", self.timeout_)
                       except Exception, e:
                           self.logger_.warn("Producer produce encount a exception: %s, " \
						"traceback: %s" % (str(e), traceback.format_exc()))
                   self.logger_.debug("ProducerWorker release queue[%d] FULL semaphore", queue_id)
                   self.framework_.queue_full_sems_[queue_id].release()
                   queue_id = (queue_id + 1) % self.framework_.thread_size_
            except Exception, e:
                self.logger_.fatal("ProducerWorker encount a exception: %s, " \
					"traceback: %s" % (str(e), traceback.format_exc()))
            finally :
                self.framework_.is_producer_running_ = False
                self.logger_.info("ProducerWorker thread exit")
                for index in xrange(0, self.framework_.thread_size_):
                    thread_id = (queue_id + index) % self.framework_.thread_size_
                    self.framework_.queue_full_sems_[thread_id].release()

    class ProcessorWorker(threading.Thread):
        def __init__(self, framework, processor, thread_id):
            threading.Thread.__init__(self)
            self.logger_ = logging.getLogger("SequentialParallelFramework.ProcessorWorker")
            self.framework_ = framework
            self.processor_ = processor
            self.thread_id_ = thread_id

        def run(self):
            try:
                self.logger_.info("ProcessorWorker thread %d start", self.thread_id_)
                while self.framework_.is_producer_running_  \
                        or not self.framework_.producer_element_queues_[self.thread_id_].isempty():
                    self.logger_.debug("ProcessorWorker[%d] wait for queue[%d] FULL semaphore" \
                                % (self.thread_id_, self.thread_id_))
                    self.framework_.queue_full_sems_[self.thread_id_].acquire()
                    self.logger_.debug("ProcessorWorker[%d] acquire queue[%d] FULL semaphore" \
                                % (self.thread_id_, self.thread_id_))
                    while not self.framework_.producer_element_queues_[self.thread_id_].isempty():
                        try:
                            p_element = self.framework_.producer_element_queues_[self.thread_id_].dequeue()
                            c_element = self.processor_.process(p_element)
                            self.framework_.consumer_element_queues_[self.thread_id_].enqueue(c_element)
                        except Exception, e:
                            self.logger_.warn("ProcessWorer[%d] process element encouter a exception: %s, " \
						"traceback: %s" % (self.thread_id_, str(e), traceback.format_exc()))
                    self.logger_.debug("ProcessorWorker[%d] release queue[%d] READY semaphore"
                                    % (self.thread_id_, self.thread_id_))
                    self.framework_.queue_ready_sems_[self.thread_id_].release()
            except Exception, e:
                self.logger_.fatal("ProcessorWorker[%d] encounter a exception: %s, " \
						"traceback: %s" % (self.thread_id_, str(e), traceback.format_exc()))
            finally:
                self.framework_.is_processor_running_[self.thread_id_] = False
                self.framework_.is_framework_running_ = False
                self.logger_.info("ProcessorWorker thread %d exit" % self.thread_id_)


    class ConsumerWorker(threading.Thread):
        def __init__(self, framework, consumer):
            threading.Thread.__init__(self)
            self.logger_ = logging.getLogger("SequentialParallelFramework.ConsumerWorker")
            self.framework_ = framework
            self.consumer_ = consumer

        def run(self):
            queue_id = 0
            try:
               self.logger_.info("ConsumerWorker thread start")
               while self.framework_.is_processor_running_[queue_id] \
                    or not self.framework_.consumer_element_queues_[queue_id].isempty():
                    self.logger_.debug("ConsumerWorker wait for queue[%d] READY semaphore" \
                           % (queue_id))
                    self.framework_.queue_ready_sems_[queue_id].acquire()
                    self.logger_.debug("ConsumerWorker acquire queue[%d] READY semaphore" \
                           % (queue_id))
                    while not self.framework_.consumer_element_queues_[queue_id].isempty():
                        try:
                            element = self.framework_.consumer_element_queues_[queue_id].dequeue()
                            self.consumer_.consume(element)
                        except Exception, e:
                            self.logger_.warn("ConsumerWorker consume a element encounter an exception: %s, " \
						"traceback: %s" % (str(e), traceback.format_exc()))
                        self.logger_.debug("ConsumerWorker release queue[%d] EMPTY semaphore" \
                            % (queue_id))
                    self.framework_.queue_empty_sems_[queue_id].release()
                    queue_id = (queue_id + 1) % self.framework_.thread_size_
            except Exception, e:
                self.logger_.fatal("ConsumerWorker encounter a exception: %s, " \
						"traceback: %s" % (str(e), traceback.format_exc()))
            finally:
                self.framework_.isConsumerRunning_ = False
                self.framework_.isFrameworkRunning_ = False
                self.logger_.info("ConsumerWorker thread exit")


    class Producer(object):
        def __init__(self):
            pass

	def produce_begin(self):
	    raise SequentialParallelFramework.UmimplementedException("Producer.produce_begin is a pure interface.")
	    
        def produce(self):
            raise SequentialParallelFramework.UmimplementedException("Producer.produce is a pure interface.")

	def produce_end(self):
	    raise SequentialParallelFramework.UmimplementedException("Producer.produce_end is a pure interface.")


    class Processor(object):
        def __init__(self):
            pass

	def process_begin(self):
	    raise SequentialParallelFramework.UmimplementedException("Processor.process_begin is a pure interface.")
	    
        def process(self):
            raise SequentialParallelFramework.UmimplementedException("Processor.process is a pure interface.")

	def process_end(self):
	    raise SequentialParallelFramework.UmimplementedException("Processor.process_end is a pure interface.")

    class Consumer(object):
        def __init__(self):
            pass

        def consume_begin(self):
            raise SequentialParallelFramework.UmimplementedException("Consumer.consume_begin is a pure interface.")

        def consume(self):
            raise SequentialParallelFramework.UmimplementedException("Consumer.consume is a pure interface.")

        def consume_end(self):
            raise SequentialParallelFramework.UmimplementedException("Consumer.consume_end is a pure interface.")

    class CircleQueue(object):
        def __init__(self, max_element_size):
            self.max_element_size_ = max_element_size
            self.head_ = 0
            self.tail_ = 0
            self.queue_size_ = 0
            self.element_array_ = [None for ix in xrange(0, self.max_element_size_)]

        def enqueue(self, e):
            if self.isfull():
                return False
            else:
                self.element_array_[self.tail_] = e
                self.tail_ = (self.tail_ + 1) % self.max_element_size_
                self.queue_size_ += 1
                return True

        def dequeue(self):
            e = None
            if not self.isempty():
                e = self.element_array_[self.head_]
                self.head_ = (self.head_ + 1) % self.max_element_size_
                self.queue_size_ -= 1
            return e

        def front(self):
            e = None
            if not self.isempty():
                e = self.element_array_[self.head_]
            return e

        def isempty(self):
            return self.queue_size_ == 0

        def isfull(self):
            return self.queue_size_ == self.max_element_size_

        def size(self):
            return self.queue_size_
