from lstore.page import Page
from lstore.record import Record

# add these internal columns into each base page and tail page because each record contains these
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2 # the time of insertion of the record (use current time from my system)
SCHEMA_ENCODING_COLUMN = 3

NUM_METADATA_COLUMNS = 4
NUM_RECORDS_PER_PAGE = 512
 
# The LogicalPage class is for handling base and tail pages (sets of physical pages)
class LogicalPage:

    # constructor
    def __init__(self, num_data_columns):
        # need number of columns a record has - use this to create a list of physical pages
        self.num_columns = num_data_columns + NUM_METADATA_COLUMNS
        # need the input columns (key + other columns)
        self.physical_pages = []
        
    # function to see if the logical page has capacity
    def has_capacity_logical(self):
        # check if one page has capacity 
        return self.physical_pages[0].has_capacity()

    # function to add record into the list of physical pages
    def insert_record(self, record):    
        # update the metadata columns
        if not self.physical_pages:
            for i in range(self.num_columns):
                self.physical_pages.append(Page())

        # go through each page in the list and add the corresponding column's value
        # -> use the "write" function from page.py
        self.physical_pages[INDIRECTION_COLUMN].write(record.indirection) 
        self.physical_pages[RID_COLUMN].write(record.rid)
        self.physical_pages[TIMESTAMP_COLUMN].write(record.timestamp)
        self.physical_pages[SCHEMA_ENCODING_COLUMN].write(record.schema)
        for i in range(4, self.num_columns):
            self.physical_pages[i].write(record.columns[i-NUM_METADATA_COLUMNS])
    
    # function to create copy of records for reading/selecting records
    def retrieve_record(self, offset, index_value, query_columns):
        # get metadata values
        record_indirection = self.physical_pages[INDIRECTION_COLUMN].read(offset)
        record_rid = self.physical_pages[RID_COLUMN].read(offset)
        record_timestamp = self.physical_pages[TIMESTAMP_COLUMN].read(offset)
        record_schema = self.physical_pages[SCHEMA_ENCODING_COLUMN].read(offset)
        # get data column values
        record_key = index_value
        record_columns = []
        # get the data columns wanted, set others to none 
        for i in range(0, len(query_columns)):
            if query_columns[i] == 1:
                record_columns.append(self.physical_pages[NUM_METADATA_COLUMNS+i].read(offset))
            else:
                record_columns.append(None)
        # return record object
        return Record(record_indirection, record_rid, record_timestamp, record_schema, record_key, record_columns)

    # function to read records for a given column index 
    def read_records(self, key_column, index_value, index_column, query_columns, page_directory, tail_pages):
        record_list = []
        # iterate through record values in column 
        for i in range(self.physical_pages[index_column].num_records): 
            # check schema to see if it is a base or tail page
            record_schema = self.physical_pages[SCHEMA_ENCODING_COLUMN].read(i)
            # base
            if record_schema == 0:
                # get the value at the column index
                record_value = self.physical_pages[index_column + NUM_METADATA_COLUMNS].read(i)
                # if value matches index value, add to list
                if record_value == index_value:
                    key_value = self.physical_pages[key_column+NUM_METADATA_COLUMNS].read(i)
                    record_list.append(self.retrieve_record(i, key_value, query_columns))
            # tail
            else:
                # check indirection of base record
                record_indirection = self.physical_pages[INDIRECTION_COLUMN].read(i)
                # get location of tail page
                tail_location = page_directory[record_indirection]
                # get value at the column index
                record_value = tail_pages[tail_location[1]].physical_pages[index_column + NUM_METADATA_COLUMNS].read(tail_location[2]) 
                # if value matches index value, add to list
                if record_value == index_value:
                    key_value = self.physical_pages[key_column+NUM_METADATA_COLUMNS].read(tail_location[2])
                    record_list.append(tail_pages[tail_location[1]].retrieve_record(tail_location[2], key_value, query_columns))
        return record_list

    
    # function for finding the indirection value for a record
    def find_indirection(self, offset):
        return self.physical_pages[INDIRECTION_COLUMN].read(offset)
    
    # function for finding the schema value for a record
    def find_schema(self, offset):
        return self.physical_pages[SCHEMA_ENCODING_COLUMN].read(offset)

    # function for finding data column values
    def find_data_columns(self, offset):
        data_columns = []
        # go through each data column to get the data values
        for i in range(4,self.num_columns):
            data_columns.append(self.physical_pages[i].read(offset))
        return data_columns

    # function for deleting a record by invalidating its rid value
    def delete_record(self, offset):
        # invalidate the record's rid by setting its value to 0
        self.physical_pages[RID_COLUMN].update(offset, 0)

    # update a single physical page(a single column) in the logical page
    # (e.g. need for updating indirection and schema columns)
    def update_column(self, new_value, column_index, offset):
        self.physical_pages[column_index].update(offset, new_value)

    # sum all values in a column 
    def sum_records(self, offset, aggregate_column_index): 
        return self.physical_pages[aggregate_column_index + NUM_METADATA_COLUMNS].read(offset)
        


