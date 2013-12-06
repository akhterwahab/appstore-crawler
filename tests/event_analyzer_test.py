import unittest

if __name__ == "__main__":
    import sys
    sys.path.append("..")

from dataPipeline import event_analyzer

__doctests__ = ['dataPipeline.event_analyzer']

class eventAnalyzerTest(unittest.TestCase):
    def testcase_analyzer_get_add_event(self):
        analyzer = event_analyzer.EventAnalyzer("event_analyzer_test.conf")
        ri = analyzer.redis_client_.get_instance()

        base_data = {"key" : "primary_key", "required_value" : "required_value",\
                "optional_value" : 1, "repeated_value" : [1, 3, 4],\
                             "ignore_value" : {"ikey" : "ivalue"}}
        add_data = {"key" : "add_key", "required_value" : "required_value",\
                "optional_value" : 1, "repeated_value" : [1, 3, 4], \
                             "ignore_value" : {"ikey" : "ivalue"}}
        ri.set(base_data["key"], analyzer.get_sign(base_data))
        self.assertTrue(analyzer.get_event(add_data) == event_analyzer.Event.ADD)
        ri.delete(base_data["key"])

    def testcase_analyzer_get_update_event(self):
        analyzer = event_analyzer.EventAnalyzer("event_analyzer_test.conf")
        ri = analyzer.redis_client_.get_instance()
        base_data = {"key" : "primary_key", "required_value" : "required_value",\
                "optional_value" : 1, "repeated_value" : [1, 3, 4],\
                             "ignore_value" : {"ikey" : "ivalue"}}

        update_data = {"key" : "primary_key", "required_value" : "update_value",\
                "optional_value" : 1, "repeated_value" : [1, 3, 4], \
                             "ignore_value" : {"ikey" : "ivalue"}} 
        ri.set(base_data["key"], analyzer.get_sign(base_data))
        self.assertTrue(analyzer.get_event(update_data) == event_analyzer.Event.UPDATE)
        ri.delete(base_data["key"])

    def testcase_analyzer_get_ignore_event(self):
        analyzer = event_analyzer.EventAnalyzer("event_analyzer_test.conf")
        ri = analyzer.redis_client_.get_instance()
        base_data = {"key" : "primary_key", "required_value" : "required_value",\
                "optional_value" : 1, "repeated_value" : [1, 3, 4],\
                             "ignore_value" : {"ikey" : "ivalue"}}

        ignore_data = {"key" : "primary_key", "required_value" : "required_value",\
                "optional_value" : 1, "repeated_value" : [1, 3, 4], \
                             "ignore_value" : {"ikey" : "cvalue"}} 

        ri.set(base_data["key"], analyzer.get_sign(base_data))
        self.assertTrue(analyzer.get_event(ignore_data) == event_analyzer.Event.IGNORE)
        ri.delete(base_data["key"])
        
if __name__ == "__main__":
    unittest.main()