class DataStore:
    '''A base class for the data store behind a sink. Derived instances of this class are passed to the sources'''
    '''on initialization so they can support update operations'''
    
    def find(self, ids):
        '''returns the document(s) matching the ids passed into this function from the store'''
        '''ids are expected to be a dictionary of key:value pairs used a filter to retrieve the documents.'''
        raise NotImplemented
    
    def list(self, keys):
        '''returns a sequence of all the IDs in the store for the sources to update or inspect'''
        '''keys are a specification of keys that can be used to locate the ids from the store'''
        raise NotImplemented
    
    def update_one(self, update_tuple):
        '''Takes a dictionary of document items to update and updates a single item in the store with the update specificication'''
        raise NotImplemented