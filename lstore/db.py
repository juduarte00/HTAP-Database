from lstore.table import Table

# counts the total number of records (base and tail) in the database
total_records = 0
base_records = 0
tail_records = 0

class Database():
    
    def __init__(self):
        self.tables = {}
        # self.total_records = 0
        # self.base_records = 0
        # self.tail_records = 0
        pass

    # Not required for milestone1
    def open(self, path):
        # read from metadata in files
        # total_records = self.total_records 
        # base_records = self.base_records 
        # tail_records = self.tail_records 
        pass

    # Not required for milestone1
    def close(self):
        # write database metadata to files
        # self.total_records = total_records
        # self.base_records = base_records 
        # self.tail_records = tail_records 
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables:
            self.tables.pop(name)
        else:
            print("Error:", name, "does not exist in the database.")

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables.get(name)
