class DataSource:
    '''The base class of data sources. The class defines the defaulkt behavior for data sources.'''
    def getUpdateItems(self):
        '''Returns a dictionary of items determined by the source for updates as opposed to add (to sinks)'''
        '''The dictionary contains key/values that represent the query to the document to update as the key.'''
        '''The query is expectted to be a vaild query to the document id. The value is the documents to update.'''
        return (None)