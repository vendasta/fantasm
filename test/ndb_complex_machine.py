""" Complex machine actions """

import random
import time
import os
import pickle
from fantasm.action import FSMAction, NDBDatastoreContinuationFSMAction
from fantasm.constants import CONTINUATION_RESULTS_KEY
from google.appengine.ext.ndb import model as ndb_model
from google.appengine.ext import db

# pylint: disable-msg=C0111
# - docstring not reqd

class NDBTestModel(ndb_model.Model):
    
    @classmethod
    def _get_kind(cls):
        """ Use the existing TestModel entities. """
        return 'TestModel'
    prop1 = ndb_model.StringProperty()
    
class ResultsModel(db.Model):
    createdTime = db.DateTimeProperty(auto_now_add=True)
    total = db.IntegerProperty()
    version = db.StringProperty()
    data = db.BlobProperty(indexed=False)

class MyDatastoreContinuationFSMAction(NDBDatastoreContinuationFSMAction):
    def getQuery(self, context, obj):
        return NDBTestModel.query().order(NDBTestModel.prop1)
    def getBatchSize(self, context, obj):
        return 2
    def execute(self, context, obj):
        if not obj[CONTINUATION_RESULTS_KEY]:
            return None
        ids = [r.key.id() for r in obj[CONTINUATION_RESULTS_KEY]]
        context['ids'] = ids
        time.sleep(5.0 * random.random())
        return 'event2'

class EntryAction1(FSMAction):
    def execute(self, context, obj):
        context['foo'] = 'bar'
        context['unicode'] = u'\xe8'
        if 'failure' in context  and random.random() < 0.25:
            raise Exception('failure')

class EntryAction2(FSMAction):
    def execute(self, context, obj):
        if 'failure' in context  and random.random() < 0.4:
            raise Exception('failure')

class EntryAction3(FSMAction):
    def execute(self, context, obj):
        pass

class EntryAction4(FSMAction):
    def execute(self, context, obj):
        pass
        
class EntryAction5(FSMAction):
    def execute(self, context, obj):
        pass

class ExitAction1(FSMAction):
    def execute(self, context, obj):
        pass
        
class ExitAction2(FSMAction):
    def execute(self, context, obj):
        pass

class ExitAction3(FSMAction):
    def execute(self, context, obj):
        pass
        
class ExitAction4(FSMAction):
    def execute(self, context, obj):
        pass
        
class ExitAction5(FSMAction):
    def execute(self, context, obj):
        pass

class DoAction1(FSMAction):
    def execute(self, context, obj):
        return 'event1'

class DoAction2(FSMAction):
    def execute(self, context, obj):
        return 'event2'
    
class DoAction3(FSMAction):
    def execute(self, context, obj):
        keys = []
        for ctx in context:
            keys.extend(ctx.get('ids', []))
        def txn():
            results = ResultsModel.get_by_key_name(context.instanceName)
            if not results:
                results = ResultsModel(key_name=context.instanceName, 
                                       total=0, 
                                       data=pickle.dumps([]),
                                       version=os.environ['CURRENT_VERSION_ID'].split('.')[0])
            results.total += len(keys)
            results.put()
        if keys:
            db.run_in_transaction(txn)
        return 'event3'
    
class DoAction4(FSMAction):
    def execute(self, context, obj):
        return 'event4'
        
class FinalAction5(FSMAction):
    def execute(self, context, obj):
        pass
    
