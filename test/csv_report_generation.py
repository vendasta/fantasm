""" Code for the csv report generation example. """


import csv
import pickle
import logging

from .fantasm.action import FSMAction
from .fantasm.action import ContinuationFSMAction
from .fantasm.action import DatastoreContinuationFSMAction
from .fantasm.constants import CONTINUATION_RESULTS_KEY
from .fantasm.constants import CONTINUATION_RESULTS_COUNTER_PARAM
from .fantasm.constants import CONTINUATION_COMPLETE_PARAM
from .fantasm.constants import TASK_NAME_PARAM
from .fantasm.constants import STEPS_PARAM
from .fantasm.lock import RunOnceSemaphore

from google.appengine.ext import db
from google.appengine.api import files

from .complex_machine import TestModel

OK_EVENT = 'ok'

NUM_ENTITIES_PARAM = 'num'
DATA_PARAM = 'data'
AGG_DATA_PARAM = 'agg-data'
COUNT_PARAM = 'count'
DONE_CONTINUATION_PARAM = 'done'
COUNTER_KEY_PARAM = 'counter-key'

DATA_KEY = 'data'
AGG_DATA_KEY = 'agg-data'

################################################################################
# POTENTIAL FRAMEWORK CODE
################################################################################

class CsvIntermediateResults( db.Model ):
    """ A model that stores a chunk of csv data """
    _data = db.BlobProperty()
    
    def getData(self):
        """ loads pickled data from _data property """
        return pickle.loads(self._data)
    
    def setData(self, data):
        """ pickles data into _data property """
        self._data = pickle.dumps(data)
        
    data = property(getData, setData)
    
class CsvProgressCounter( db.Model ):
    """ A model that tracks the aggregation process """
    counter = db.IntegerProperty(indexed=False)
    expectedCounter = db.IntegerProperty(indexed=False)

class CsvContinuation( DatastoreContinuationFSMAction ):
    """ An FSMAction that extracts CSV data from a datastore continuation """
    
    def execute(self, context, obj):
        """ Processes data from a datastore continuation """
        
        # if there are results, then processthem, otherwise stop the machine
        if obj.get(CONTINUATION_RESULTS_KEY):
            
            entities, data = obj[CONTINUATION_RESULTS_KEY], []
            
            # process each entity in turn
            for entity in entities:
                
                # extracting entity specific data
                csvData = self.generateCsvData(context, obj, entity)
                if csvData:
                    data.append(csvData)
                
            if data:
                context[DATA_PARAM] = data
                
            # and then extracting data from a batch of entities (ie. sums, averages etc.)
            aggData = self.generateAggregatedCsvData(context, obj, entities)
            if aggData:
                context[AGG_DATA_PARAM] = aggData
            
            # this stores the total number of entities processed by this Task
            context[NUM_ENTITIES_PARAM] = len(entities)
            
        # keep going in all cases, even when the continuation is done
        return OK_EVENT
        
    def generateCsvData(self, context, obj, entity):
        """ Returns a pickle-able python object, containing data to be used in generating a CSV file
        
        @param context: an FSMContext
        @param obj: a temporary state object
        @param entity: a db.Model instance
        """
        raise NotImplementedError()
    
    def generateAggregatedCsvData(self, context, obj, entities):
        """ Returns a pickle-able python object, containing data to be used in generating a CSV file
        
        @param context: an FSMContext
        @param obj: a temporary state object
        @param entities: a list of db.Model instances
        """
        raise NotImplementedError()
    
class CsvFanIn( FSMAction ):
    """ An FSMAction fan-in that extracts CSV data from batches of data """
    
    def execute(self, contexts, obj):
        """ Processes data from a datastore continuations, via fan-in """
        
        data, aggData = [], None
        
        # loop over all the contexts, appending data to lists
        for context in contexts:
            if context.get(DATA_PARAM):
                data.extend(context[DATA_PARAM])
            if context.get(AGG_DATA_PARAM):
                aggData = self.mergeAggregatedCsvData(contexts, obj, aggData, context[AGG_DATA_PARAM]) if aggData else context[AGG_DATA_PARAM]
        
        # save the intermediate results, and if done, transition to the csv writer
        done = self.saveIntermediateResults(contexts, obj, data, aggData)
        if done:
            return OK_EVENT
        
    def saveIntermediateResults(self, contexts, obj, data, aggData):
        """ Saves intermediate results, and updates a progress counter """
        
        # use a transaction so the results and counter match
        def txn():
            """ txn """
            
            context = contexts[0] # all have same instanceName
            
            # check to see if we have fanned in the final continuation, and know
            # the final number of results that were processed
            expectedCounter = ContinuationFSMAction.checkFanInForTotalResultsCount(contexts, obj)
            
            # fetch the current counter
            counter = CsvProgressCounter.get_by_key_name(context.instanceName)
            if not counter:
                counter = CsvProgressCounter(key_name=context.instanceName, 
                                             counter=0)
            counter.counter += sum([ContinuationFSMAction.getResultsCount(context, obj) for context in contexts])
            counter.expectedCounter = expectedCounter
            toPut = [counter]
            
            # create an intermediate record
            if data:
                ir = CsvIntermediateResults(key_name=obj[TASK_NAME_PARAM], parent=counter)
                ir.data = data
                toPut.append(ir)
            
            # update an aggregated record
            if aggData:
                air = CsvIntermediateResults.get_by_key_name(context.instanceName, counter)
                if not air:
                    air = CsvIntermediateResults(key_name=context.instanceName, parent=counter)
                    air.data = aggData
                toPut.append(air)
                
            # put all three in a transaction
            db.put(toPut)
            
            # this is the flag to indicated done
            return counter.expectedCounter and counter.counter >= counter.expectedCounter
                
        # return whether done or not
        return db.run_in_transaction(txn)
    
    def mergeAggregatedCsvData(self, contexts, obj, aggData1, aggData2):
        """ Merges aggregated data """
        raise NotImplementedError()
    
class CsvWriter( FSMAction ):
    """ A simple .csv writing class that writes everything in a single Task.
    
        NOTE: this does not scale arbitrarily, since we are constrained to a single Task
        TODO: we could fan-out over CsvIntermediateResults and then write batches via fan-in,
              although that seems unnecessary
        """
    
    def execute(self, context, obj):
        """ Writes the CSV file """
        
        # if already wrote a file, don't do anything
        semaphore = RunOnceSemaphore(context.instanceName, context)
        if not semaphore.readRunOnceSemaphore(payload='payload'):
        
            # fetch the CsvCounter, since it is the parent of all the other Models
            counter = CsvProgressCounter.get_by_key_name(context.instanceName)
            # fetch the single aggregated results Model
            aggResults = CsvIntermediateResults.get_by_key_name(context.instanceName, counter)
            
            # open the file
            fileName = files.blobstore.create(mime_type='application/octet-stream')
            with files.open(fileName, 'a') as f:
                
                # the csv module has a convenient row writing interface
                writer = csv.writer(f)
                
                # this queries for all the intermediate results
                query = CsvIntermediateResults.all().ancestor(counter)
                for results in query:
                    
                    # the aggregated results may also be in the results, so skip them
                    if aggResults and results.key() == aggResults.key():
                        continue
                    
                    # for all the intermediate data, write the rows
                    data = results.data
                    for item in data:
                        rows = self.getRows(context, obj, item, aggResults.data)
                        if rows:
                            for row in rows:
                                writer.writerow(row)
                
                if aggResults:
                    # now also write down any specific aggregated data rows
                    rows = self.getAggregatedRows(context, obj, aggResults.data)
                    if rows:
                        for row in rows:
                            writer.writerow(row)
            
            # finalize the file
            files.finalize(fileName)
            
            # FIXME: what to do with this?
            blobKey = files.blobstore.get_blob_key(fileName)
            
            # at this point we have successfully written the file, lets make sure we don't do it again
            # if a retry occurs downstream
            semaphore.writeRunOnceSemaphore(payload='payload')
            
        # store the key of the counter (ie. parent of intermediate results) for cleanup
        context[COUNTER_KEY_PARAM] = counter.key()
        return OK_EVENT
    
    def getRows(self, context, obj, data, aggData):
        """ Returns a list of lists corresponding to a rows in the CSV """
        raise NotImplementedError()

    def getAggregatedRows(self, context, obj, aggData):
        """ Returns a list of lists corresponding to a rows in the CSV """
        raise NotImplementedError()
    
class CsvCleanup( DatastoreContinuationFSMAction ):
    """ Cleans up all the intermediate data and counters """
    
    def getQuery(self, context, obj):
        """ Returns a query for CsvIntermediateResults that need deletion """
        counterKey = context[COUNTER_KEY_PARAM]
        return CsvIntermediateResults.all(keys_only=True).ancestor(counterKey)
    
    def getBatchSize(self, context, obj):
        """ Number of CsvIntermediateResults to delete per Task """
        return 100
    
    def execute(self, context, obj):
        """ deletes the CsvIntermediateResults until none remain, and then deletes the CsvProgressCounter """
        if obj.get(CONTINUATION_RESULTS_KEY):
            db.delete(obj[CONTINUATION_RESULTS_KEY])
            if len(obj[CONTINUATION_RESULTS_KEY]) < self.getBatchSize(context, obj):
                counter = CsvProgressCounter.get_by_key_name(context.instanceName)
                if counter:
                    counter.delete()
        else:
            counter = CsvProgressCounter.get_by_key_name(context.instanceName)
            if counter:
                counter.delete()
    
################################################################################
# USER CODE
################################################################################

class ReadInputRecords( CsvContinuation ):
    
    def getQuery(self, context, obj):
        """ These TestModel instances are also used by ComplexMachine """
        return TestModel.all()
    
    def getBatchSize(self, context, obj):
        """ Read 10 records at a time """
        return 10
    
    def generateCsvData(self, context, obj, entity):
        """ Return some simple info re: the entity """
        return [str(entity.key()), entity.prop1]
    
    def generateAggregatedCsvData(self, context, obj, entities):
        """ Return the sum of the prop1 field (guids cast to ints)"""
        return sum([int(e.prop1.replace('-', ''), 16) for e in entities])
    
class WriteIntermediateRecords( CsvFanIn ):
    
    def mergeAggregatedCsvData(self, contexts, obj, aggData1, aggData2):
        """ Merges aggregated data - basically adds the two values. If something
            more complicated is required, you can do that too. """
        return aggData1 + aggData2
    
class WriteCsvReport( CsvWriter ):
    
    def getRows(self, context, obj, data, aggData):
        """ Returns a list corresponding to a row in the CSV """
        rowNum = obj.get('rowNum', 1)
        obj['rowNum'] = rowNum + 1
        return [[rowNum] + data + [aggData]]
    
    def getAggregatedRows(self, context, obj, aggData):
        """ Returns a list corresponding to a row in the CSV """
        return []