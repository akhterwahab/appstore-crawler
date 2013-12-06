from framework.sequential_parallel_framework import SequentialParallelFramework 
import json

class DataProcessor(SequentialParallelFramework.Processor):
    class InvalidTypeException(Exception): pass

    def __init__(self, conf_path):
        SequentialParallelFramework.Processor.__init__(self)
        

            
        
        

    def process(self, element):
        jo = json.loads(element)
        if not duplicate_check(jo):
        keys = self.sign_fields_
        
        
        
        

        
        
        
        

   
        

 
        
