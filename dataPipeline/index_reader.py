class IndexFileReader(object):
    def __init__(self, filepath):
        self.filepath_ = filepath
        self.fp_ = open(filepath)

    def close(self):
        self.fp_.close()
    
    def read(self):
        line = self.fp_.readline()
        if line:
            record = line.replace("\n", "").split("\t")
        else:
            record = None
        if record:
            return (record[0], int(record[1]), set(record[2:]))
        else:
            return (None, 0, [])

