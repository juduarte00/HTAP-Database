from lstore.LogicalPage import LogicalPage, INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN

BASE_PAGES_PER_PAGE_RANGE = 16

class PageRange:

    #constructor
    def __init__(self, num_data_columns):
        self.base_pages = []
        self.tail_pages = []
        self.num_data_columns = num_data_columns

    # function to check if there is space in the base pages 
    def has_capacity_base(self):
        if len(self.base_pages) == BASE_PAGES_PER_PAGE_RANGE:
            return self.base_pages[-1].has_capacity_logical()
        # check if we reached the maximum number of base pages allowed
        return len(self.base_pages) < BASE_PAGES_PER_PAGE_RANGE

    # function to check if there is space in the tail pages
    def has_capacity_tail(self):
        return self.tail_pages[-1].has_capacity_logical()
    
    # function to find indirection of a base page
    def find_indirection_base(self, pageid, offset):
        return self.base_pages[pageid].find_indirection(offset)

    # function to find indirection of a tail page
    def find_indirection_tail(self, pageid, offset):
        return self.tail_pages[pageid].find_indirection(offset)

    # function to find the schema of a base page
    def find_schema(self, pageid, offset):
        return self.base_pages[pageid].find_schema(offset)

    # function to get data columns
    def find_data_columns(self, isBase, pageid, offset):
        if isBase:
            return self.base_pages[pageid].find_data_columns(offset)
        else:
            return self.tail_pages[pageid].find_data_columns(offset)

    # function to insert records into base pages
    def insert_record_base(self, record):
        if not self.base_pages:
            self.base_pages.append(LogicalPage(self.num_data_columns))
        elif not (self.base_pages[-1].has_capacity_logical()):
            self.base_pages.append(LogicalPage(self.num_data_columns))
        self.base_pages[-1].insert_record(record)

    # function to insert record into tail pages
    def insert_record_tail(self, record):
        if not self.tail_pages:
            self.tail_pages.append(LogicalPage(self.num_data_columns))
        elif not (self.tail_pages[-1].has_capacity_logical()):
            self.tail_pages.append(LogicalPage(self.num_data_columns))
        self.tail_pages[-1].insert_record(record)

    # update the indirection/schema of the original record in the base page
    def update_base_record(self, new_indirection, new_schema, pageid, offset):
        self.base_pages[pageid].update_column(new_indirection, INDIRECTION_COLUMN, offset)
        self.base_pages[pageid].update_column(new_schema, SCHEMA_ENCODING_COLUMN, offset)

    # function for deleting a record in the base page by invalidating the 
    def delete_base_record(self, pageid, offset):
        # invalidate the rid of the original record in the base page
        self.base_pages[pageid].delete_record(offset)
    
     # function for deleting a record in the base page by invalidating the 
    def delete_tail_record(self, pageid, offset):
        # invalidate the rid of the original record in the base page
        self.tail_pages[pageid].delete_record(offset)
    
    # function to read base pages for select 
    def read_base_pages(self, key_column, index_value, index_column, query_columns, page_directory):
        base_num = len(self.base_pages)
        record_list = []
        for i in range (base_num):
            new_records = self.base_pages[i].read_records(key_column, index_value, index_column, query_columns, page_directory, self.tail_pages)
            if new_records: 
                record_list.extend(new_records)
        return record_list
    
    # function to sum values in a column depending on whether it is in base or tail pages 
    def sum_column(self, pageid, offset, isBase, aggregate_column_index):
        if isBase:
            return self.base_pages[pageid].sum_records(offset, aggregate_column_index)
        else:
            return self.tail_pages[pageid].sum_records(offset, aggregate_column_index)
        
