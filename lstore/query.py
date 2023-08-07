from lstore.record import Record
import lstore.db as db
from time import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        return self.table.delete_record(primary_key)
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        # start the rid at 1 so 0 can be the value used to invalidate the rids
        record_rid = db.total_records + 1
        # increment the number of base records with the insertion of a new base record
        db.base_records = db.base_records + 1
        # always increment the number of records after a new record is inserted into the database
        db.total_records = db.total_records + 1
        # initial indirection of a record is just itself
        record_indirection = record_rid
        # timestamp - number of seconds since the epoch
        record_timestamp = int(time())
        # schema_encoding - initially no columns are changed
        schema_encoding = '0' * self.table.num_columns
        # convert encoding into an integer
        int_schema_encoding = int(schema_encoding, 2)
        # record's key column value
        record_key_value = columns[self.table.key]
        # create a new Record object
        new_record = Record(record_indirection, record_rid, record_timestamp, int_schema_encoding, record_key_value, columns)
        # insert record into table
        self.table.insert_record(new_record, db.base_records)
        return True

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    # Returns the specified set of columns from the record with the given key 
    """
    
    def select(self, index_value, index_column, query_columns):
        return self.table.select_record(index_value, index_column, query_columns)


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        # have to + 1 because rids at 1 so 0 can be the value used to invalidate the rids
        record_rid = db.total_records + 1
        # increment amount of tail records
        db.tail_records = db.tail_records + 1
        # increment amount of total records
        db.total_records = db.total_records + 1
        # set temporary indirection number to be fixed in table class
        record_indirection = None 
        # timestamp - number of seconds since the epoch
        record_timestamp = int(time())
        # schema_encoding - only 1's for values that are changed
        schema_encoding = ''
        for i in range(self.table.num_columns):
            # values that haven't changed are None in the 'columns' parameter
            if columns[i] == None:
                schema_encoding = schema_encoding + '0'
            # values that have changed
            else:
                schema_encoding = schema_encoding + '1'
        int_schema_encoding = int(schema_encoding, 2)
        # record's key column value
        record_key_value = primary_key
        # create new record to be added into tail page
        updated_record = Record(record_indirection, record_rid, record_timestamp, int_schema_encoding, record_key_value, columns)
        # update record
        return self.table.update_record(primary_key, updated_record)
    

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
       return self.table.sum_records(start_range, end_range, aggregate_column_index)

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
