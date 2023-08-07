class Record:

    def __init__(self, indirection, rid, timestamp, schema, key, columns):
        # metadata information
        self.indirection = indirection
        self.rid = rid
        self.timestamp = timestamp
        self.schema = schema
        # value in the key column
        self.key = key
        # list for the values in each of the columns
        self.columns = columns