""" FSMActions used in unit tests """

from fantasm.action import NDBDatastoreContinuationFSMAction
from google.appengine.ext.ndb import model as ndb_model
from fantasm.constants import FORK_PARAM
from fantasm.constants import CONTINUATION_RESULT_KEY
from fantasm.constants import CONTINUATION_RESULTS_KEY

# pylint: disable=C0111, W0613
# - docstrings not reqd in unit tests
# - these actions do not use arguments

class NDBTestModel(ndb_model.Model):
    prop1 = ndb_model.StringProperty()

class CountExecuteCalls(object):
    def __init__(self):
        self.count = 0
        self.fails = 0
    def execute(self, context, obj):
        self.count += 1
        if self.fails:
            self.fails -= 1
            raise Exception()
        return self.event
    @property
    def event(self):
        return 'next-event'

class CountExecuteCallsFinal(CountExecuteCalls):
    @property
    def event(self):
        return None

class TestDatastoreContinuationFSMAction(NDBDatastoreContinuationFSMAction):
    def __init__(self):
        super(TestDatastoreContinuationFSMAction, self).__init__()
        self.count = 0
        self.ccount = 0
        self.fails = 0
        self.failat = 0
        self.cfailat = 0
    def getQuery(self, context, obj):
        return NDBTestModel.query().order(NDBTestModel.prop1)
    def getBatchSize(self, context, obj):
        return 2
    def continuation(self, context, obj, token=None):
        self.ccount += 1
        if self.ccount == self.cfailat:
            raise Exception()
        return super(TestDatastoreContinuationFSMAction, self).continuation(context, obj, token=token)
    def execute(self, context, obj):
        if not obj[CONTINUATION_RESULTS_KEY]:
            return None
        self.count += 1
        context['__count__'] = self.count
        context['fan-me-in'] = context.get('fan-me-in', []) + [r.key for r in obj[CONTINUATION_RESULTS_KEY]]
        if self.count == self.failat:
            raise Exception()
        if self.fails:
            self.fails -= 1
            raise Exception()
        return 'next-event'

class TestContinuationAndForkFSMAction(NDBDatastoreContinuationFSMAction):
    def __init__(self):
        super(TestContinuationAndForkFSMAction, self).__init__()
        self.count = 0
        self.ccount = 0
        self.fails = 0
        self.failat = 0
        self.cfailat = 0
    def getQuery(self, context, obj):
        return NDBTestModel.query().order(NDBTestModel.prop1)
    def getBatchSize(self, context, obj):
        return 2
    def continuation(self, context, obj, token=None):
        self.ccount += 1
        if self.ccount == self.cfailat:
            raise Exception()
        return super(TestContinuationAndForkFSMAction, self).continuation(context, obj, token=token)
    def execute(self, context, obj):
        if not obj[CONTINUATION_RESULTS_KEY]:
            return None
        self.count += 1
        context['__count__'] = self.count
        context['data'] = {'a': 'b'}
        if self.count == self.failat:
            raise Exception()
        if self.fails:
            self.fails -= 1
            raise Exception()

        # FIXME: this pattern is a bit awkward, or is it?
        # FIXME: how can we drive this into yaml?
        # FIXME: maybe just another provided base class like DatastoreContinuationFSMAction?

        # fork a machine to deal with all but one of the continuation dataset
        for result in obj[CONTINUATION_RESULTS_KEY][1:]:
            context.fork(data={'key': result.key})

        # and deal with the leftover data item
        context['key'] = obj[CONTINUATION_RESULT_KEY].key
        context[FORK_PARAM] = -1

        # this event will be dispatched to this machine an all the forked contexts
        return 'next-event'
