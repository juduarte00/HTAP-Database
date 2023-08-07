ENTRY_SIZE = 8
NUM_RECORDS_PER_PAGE = 512

# This class is for handling PHYSICAL pages
class Page:

    def __init__(self):
        self.num_records = 0
        # an array that has a length of 4096 and each element is 1 byte large
        self.data = bytearray(4096)

    # check if the page still has space to insert new records
    def has_capacity(self):
        return self.num_records < NUM_RECORDS_PER_PAGE

    # insert the given value into the page and update the number of records in the page
    def write(self, value):
        startIndex = self.num_records * ENTRY_SIZE
        endIndex = startIndex + ENTRY_SIZE
        # value is an 64-bit integer so must convert into 8 bytes
        if value is not None:
            self.data[startIndex:endIndex] = value.to_bytes(8, byteorder = 'big', signed = True)
            self.num_records += 1

    # function for retrieving an entry's value from its physical location in the page
    def read(self, offset):
        startIndex = offset * ENTRY_SIZE
        endIndex = startIndex + ENTRY_SIZE
        # must convert the 8 bytes of data back into a 64-bit integer
        return int.from_bytes(self.data[startIndex:endIndex], byteorder='big', signed = True)

    # function for updating an entry's value
    # (used for updating indirection and schema values in physical pages)
    def update(self, offset, value):
        startIndex = offset * ENTRY_SIZE
        endIndex = startIndex + ENTRY_SIZE
        self.data[startIndex:endIndex] = value.to_bytes(8, byteorder = 'big', signed = True)