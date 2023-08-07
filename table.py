from lstore.PageRange import PageRange
from lstore.index import Index
from lstore.LogicalPage import SCHEMA_ENCODING_COLUMN, INDIRECTION_COLUMN
import math


# have a maximum of 16 base pages per page range, which gives us 512 * 16 = 8192 records/page range
NUM_RECORDS_PER_PAGE_RANGE = 8192
# records per base or tail page
NUM_RECORDS_PER_PAGE = 512
# number of base pages per page range
BASE_PAGES_PER_PAGE_RANGE = 16

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        # dictionary with key = rid, value = [pagerangeid, pageid, offset]
        self.page_directory = {}
        # dictionary for mapping each record's primary key value to rid
        self.keyToRID = {}
        # list for the page ranges
        self.page_ranges = []

        # IMPLEMENT IN MILESTONE 2
        self.index = Index(self)

    # (S2) BUFFERPOOL MANAGEMENT 
    # function for finding the page range a record is located in
    def get_page_range_id(self,num_base_records):
        return math.floor((num_base_records - 1)/ NUM_RECORDS_PER_PAGE_RANGE)

    # function for finding the page a base record is located in
    def get_base_page_id(self, num_base_records):
        return ((math.floor((num_base_records - 1)/ NUM_RECORDS_PER_PAGE)) % BASE_PAGES_PER_PAGE_RANGE)
    
    # function for finding the page a tail record is located in
    def get_tail_page_id(self, pagerangeid):
        # because there isn't a set number of tail pages per page range,
        # find how many tail pages there are currently minus 1 and that is the pageid for the new tail record being added
        return (len(self.page_ranges[pagerangeid].tail_pages) - 1)

    # function for finding the offset of a base page
    def get_base_offset(self, num_base_records):
        return ((num_base_records - 1) % NUM_RECORDS_PER_PAGE) 
    
    # function for finding the offset of a tail page
    def get_tail_offset(self, pagerangeid):
        return (self.page_ranges[pagerangeid].tail_pages[-1].physical_pages[0].num_records - 1)

    # function for inserting a new record into the table
    def insert_record(self, record, num_base_records):
        # add mapping of primary key to rid
        self.keyToRID[record.key] = record.rid
        # add record's rid to the page directory
        self.page_directory[record.rid] = [self.get_page_range_id(num_base_records),
            self.get_base_page_id(num_base_records), self.get_base_offset(num_base_records)]
        # check if list is empty
        if not self.page_ranges:
            self.page_ranges.append(PageRange(self.num_columns))
        # check if there is no space left in the current page range
        elif not (self.page_ranges[-1].has_capacity_base()):
            self.page_ranges.append(PageRange(self.num_columns))
        # insert record into next slot available in the current page range we are on
        self.page_ranges[-1].insert_record_base(record)

    # function for deleting a record inside the table
    def delete_record(self, primary_key):
        # if primary key is not in keys, return false
        if primary_key in self.keyToRID.keys():
            # get RID from primary key
            base_rid = self.keyToRID[primary_key]
        else:
            return False
        # get location of rid in base pages 
        base_record_location = []
        # check if base_rid is in page directory
        if base_rid in self.page_directory.keys():
            base_record_location = self.page_directory[base_rid]
        else:
            return False
        pagerangeid = base_record_location[0]
        base_pageid = base_record_location[1]
        base_offset = base_record_location[2]
        # check for tail pages by looking at the indirection column
        tail_record_rid = self.page_ranges[pagerangeid].find_indirection_base(base_pageid, base_offset)
        # loop through the tail pages
        while tail_record_rid != base_rid:
            # get the location of tail pages
            tail_page_location = self.page_directory.get(tail_record_rid)
            if tail_page_location:
                 tail_pageid = tail_page_location[1]
                 tail_offset = tail_page_location[2]
            else:
                break
            # delete tail page
            self.page_ranges[pagerangeid].delete_tail_record(tail_pageid, tail_offset)
            # remove tail page from page_directory
            self.page_directory.pop(tail_record_rid)
            # get indirection of any previous tail pages
            tail_record_rid = self.page_ranges[pagerangeid].find_indirection_tail(tail_pageid, tail_offset)
                
        # delete base page
        self.page_ranges[pagerangeid].delete_base_record(base_pageid, base_offset)
        # remove base page from page_directory
        self.page_directory.pop(base_rid)
        # remove the mapping from key to rid because no longer needed for this base record
        self.keyToRID.pop(primary_key)
        return True
    
    # function for selecting and reading records 
    def select_record(self, index_value, index_column, query_columns):
        record_list = []
        # looking for value in the key column
        if self.key == index_column:
            # get RID
            base_rid = self.keyToRID[index_value]
            # get location
            base_location = self.page_directory[base_rid]
            # get the schema of the base record to see if record has tail pages
            base_record_schema = self.page_ranges[base_location[0]].base_pages[base_location[1]].physical_pages[SCHEMA_ENCODING_COLUMN].read(base_location[2])
            if base_record_schema == 0:
                # create record object from base record if it does not have any tail pages
                new_record = self.page_ranges[base_location[0]].base_pages[base_location[1]].retrieve_record(base_location[2], index_value, query_columns)
            else:
                # create record object from tail record so it is the most updated version
                tail_rid = self.page_ranges[base_location[0]].base_pages[base_location[1]].physical_pages[INDIRECTION_COLUMN].read(base_location[2])
                # get tail location 
                tail_location = self.page_directory[tail_rid]
                new_record = self.page_ranges[tail_location[0]].tail_pages[tail_location[1]].retrieve_record(tail_location[2], index_value, query_columns)
            if new_record:
                record_list.append(new_record)
        else: 
            # looking for value in columns other than key
            # must go through all the base pages and if needed, its corresponding tail pages for the index value
            for i in range (len(self.page_ranges)):
                new_record = self.page_ranges[i].read_base_pages(self.key, index_value, index_column, query_columns, self.page_directory)
            if new_record:
                record_list.extend(new_record)       
        return record_list
                                           
    # function for updating a record inside the table
    def update_record(self, primary_key, record):
        # get the base_rid to update the indirection/schema of the original record in the base page
        if primary_key in self.keyToRID.keys():
            base_rid = self.keyToRID[primary_key]
        else:
            # return false if no records exist with given key
            return False
        # get location of base record 
        base_record_location = self.page_directory[base_rid]
         # updates belong in the same page range as their base record
        pagerangeid = base_record_location[0]
        base_pageid = base_record_location[1]
        base_offset = base_record_location[2]
        # set new tail record indirection to the rid of the previous version of the record
        # (if no other tail records, then its indirection is just the rid of the original base record)
        record.indirection = self.page_ranges[pagerangeid].find_indirection_base(base_pageid, base_offset)
        # get location of the record's previous version
        previous_record_location = self.page_directory[record.indirection]
        previous_record_pageid = previous_record_location[1]
        previous_record_offset = previous_record_location[2]
        # boolean used as a parameter in the find_data_columns function
        isBaseRecord = True
        if record.indirection != base_rid:
            isBaseRecord = False
        previous_record_data_columns = self.page_ranges[pagerangeid].find_data_columns(isBaseRecord, previous_record_pageid, previous_record_offset)
        # make sure all the None values are updated to values from the previous version of the record
        # (cumulative tail record)
        new_columns = []
        for i in range(self.num_columns):
            if record.columns[i] == None:
                new_columns.append(previous_record_data_columns[i])
            else:
                new_columns.append(record.columns[i])
        record.columns = new_columns
        # tail pages will always fit in the page range
        self.page_ranges[pagerangeid].insert_record_tail(record)
        # set base_rid schema to 1 for corresponding updated columns
        old_base_schema_int = self.page_ranges[pagerangeid].base_pages[base_pageid].find_schema(base_offset)
        tail_schema_int = record.schema
        # do the bit OR operation on the two binary strings to get the new base schema
        new_base_schema = old_base_schema_int | tail_schema_int
        # make sure to update the indirection and schema values for the original base record 
        self.page_ranges[pagerangeid].update_base_record(record.rid, new_base_schema, base_pageid, base_offset)
        # add to new tail record's rid to the page directory
        self.page_directory[record.rid] = [pagerangeid, self.get_tail_page_id(pagerangeid), self.get_tail_offset(pagerangeid)]
        return True

    # function to sum the value of records across a range 
    def sum_records(self, start_range, end_range, aggregate_column_index):
        total = 0
        hasNoRecords = True
        # iterate from the beginning to end of range
        while (start_range <= end_range):
            # get the rid of each key in the range
            if start_range in self.keyToRID.keys():
                start_rid = self.keyToRID[start_range] # base record's rid
                hasNoRecords = False
            else:
                start_range = start_range + 1
                continue
            # get location of base record
            record_location = self.page_directory[start_rid]
            pagerangeid = record_location[0]
            pageid = record_location[1]
            offset = record_location[2]
            # check if record is base or tail page 
            tail_rid = self.page_ranges[pagerangeid].base_pages[pageid].find_indirection(offset)
            tail_location = self.page_directory[tail_rid]
            isBaseRecord = True
            # if tail page, overwrite pageid and offset values
            if tail_rid != start_rid:
                isBaseRecord = False
                pageid = tail_location[1]
                offset = tail_location[2]
            # sum pages 
            total = total + self.page_ranges[pagerangeid].sum_column(pageid, offset, isBaseRecord, aggregate_column_index)
            # increment key
            start_range = start_range + 1
        if hasNoRecords:
            return False
        return total 

    # IMPLEMENT IN MILESTONE 2
    def __merge(self):
        print("merge is happening")
        pass
 
