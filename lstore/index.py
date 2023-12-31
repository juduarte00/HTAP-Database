"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        # INDEXING IMPLEMENT IN MILESTONE 2
        self.indices = [None] * table.num_columns

    """
    # returns the location of all records with the given value on column "column"
    """
    # NOT SURE IF THIS IS IMPLEMENTED IN MILESTONE 1
    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    # NOT SURE IF THIS IS IMPLEMENTED IN MILESTONE 1
    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """
    # IMPLEMENT IN MILESTONE 2
    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """
    # IMPLEMENT IN MILESTONE 2
    def drop_index(self, column_number):
        pass
