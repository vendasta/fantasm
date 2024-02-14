""" Tests for fantasm.fsm """
import logging
import time
import unittest
import urllib.request, urllib.parse, urllib.error
import datetime
import sys

if sys.version_info < (2, 7):
    import simplejson as json
else:
    import json

import random # pylint: disable=W0611
import pickle
from google.appengine.api.taskqueue.taskqueue import Queue, Task # pylint: disable=W0611
from google.appengine.api import memcache # pylint: disable=W0611
from google.appengine.ext import db
from google.appengine.ext.ndb import model as ndb_model, key as ndb_key
from fantasm import config
from fantasm.handlers import TemporaryStateObject
from fantasm.fsm import FSMContext, FSM, startStateMachine
from fantasm.transition import Transition
from fantasm.exceptions import UnknownMachineError, UnknownStateError, UnknownEventError, \
                               FanInWriteLockFailureRuntimeError, \
                               YamlFileCircularImportError
from fantasm.state import State
from fantasm.models import _FantasmFanIn
from fantasm.constants import STATE_PARAM, EVENT_PARAM, INSTANCE_NAME_PARAM, STEPS_PARAM, MACHINE_STATES_ATTRIBUTE, \
                              CONTINUATION_PARAM, INDEX_PARAM, GEN_PARAM, FORKED_CONTEXTS_PARAM, \
                              FORK_PARAM, TASK_NAME_PARAM, RETRY_COUNT_PARAM, CONTINUATION_RESULTS_KEY, \
                              HTTP_REQUEST_HEADER_QUEUENAME
from fantasm_tests.fixtures import AppEngineTestCase
from fantasm_tests.actions import RaiseExceptionAction, RaiseExceptionContinuationAction
from fantasm_tests.helpers import TaskQueueDouble, getLoggingDouble
from fantasm_tests.helpers import ConfigurationMock
from fantasm_tests.helpers import getFSMFactoryByFilename
from fantasm_tests.helpers import getMachineNameByFilename
from fantasm_tests.helpers import setUpByFilename
from fantasm_tests.helpers import getCounts
from fantasm_tests.actions import CountExecuteCalls
from fantasm_tests.actions import CountExecuteCallsWithFork

from minimock import mock, restore

# pylint: disable=C0111, W0212, W0612, W0613
# - docstrings not reqd in unit tests
# - unit tests need access to protected members
# - lots of unused args in unit tests

NAMESPACED_EVENT_MODULE_LEVEL_FSM_TESTS = 'NAMESPACED-EVENT-MODULE-LEVEL-FSM-TESTS'
NAMESPACED_CONTEXT_TYPE_MODULE_LEVEL_FSM_TESTS = 'NAMESPACED-CONTEXT-TYPE-MODULE-LEVEL-FSM-TESTS'

class FSMTests(unittest.TestCase):

    NAMESPACED_EVENT_CLASS_LEVEL_FSM_TESTS = 'NAMESPACED-EVENT-CLASS-LEVEL-FSM-TESTS'
    NAMESPACED_CONTEXT_TYPE_CLASS_LEVEL_FSM_TESTS = 'NAMESPACED-CONTEXT-TYPE-CLASS-LEVEL-FSM-TESTS'

    def test(self):
        machineName = 'foo'
        machineConfig = config._MachineConfig({'name':machineName})
        machineConfig.addState({'name':'foo', 'initial': True, 'action': 'fantasm_tests.actions.CountExecuteCalls'})
        currentConfig = ConfigurationMock(machines=[machineConfig])
        factory = FSM(currentConfig=currentConfig)
        context = factory.createFSMInstance('foo')

    def test_TaskQueueFSMRetryTests(self):
        setUpByFilename(self, 'test-TaskQueueFSMRetryTests.yaml')

    def test_TaskQueueFSMTests(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')

    def test_createFSMInstance_raises_UnknownMachineError(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        self.assertRaises(UnknownMachineError, self.factory.createFSMInstance, 'foo')

    def test_createFSMInstance_raises_UnknownStateError_for_currentState(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        self.assertRaises(UnknownStateError, self.factory.createFSMInstance, 'TaskQueueFSMTests',
                          currentStateName='foo')

    def test_TestYamlFileLocation(self):
        setUpByFilename(self, 'test-TestYamlFileLocation.yaml', machineName='MyMachine')

    def test_transitionRetryPolicyOverridesMachineLevelPolicy(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        transInitialToNormal = self.initialState._eventToTransition['next-event']
        self.assertNotEqual(self.machineConfig.taskRetryLimit, transInitialToNormal.retryOptions.task_retry_limit)

    def test_createFSMInstance_no_initial_data(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        context = self.factory.createFSMInstance('TaskQueueFSMTests')
        self.assertEqual({}, context)

    def test_createFSMInstance_initial_data(self):
        setUpByFilename(self, 'test-TaskQueueFSMTests.yaml')
        context = self.factory.createFSMInstance('TaskQueueFSMTests', data={'a' : 'b'})
        self.assertEqual({'a' : 'b'}, context)

class FSMContextTests(unittest.TestCase):

    def setUp(self):
        super().setUp()
        filename = 'test-FSMContextTests.yaml'
        setUpByFilename(self, filename)
        self.machineName = getMachineNameByFilename(filename)
        self.mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=self.mockQueue.add, tracker=None)
        # dispatch initial event to get context in correct state
        self.taskName = 'foo'
        self.obj = {TASK_NAME_PARAM: self.taskName}
        self.context.dispatch(FSM.PSEUDO_INIT, self.obj)

    def tearDown(self):
        super().tearDown()
        restore()

    def getContextWithoutSpecialEntries(self):
        context = self.context.clone()
        for key in list(context.keys()):
            if key.startswith('__') and key.endswith('__'):
                context.pop(key)
        return context

    def test_contextValueSet(self):
        self.context['foo'] = 'bar'
        self.assertEqual(self.context.get('foo'), 'bar')

    def test_contextSetQueue(self):
        queue = 'some-queue'
        self.context.setQueue(queue)
        self.assertEqual(self.context.headers[HTTP_REQUEST_HEADER_QUEUENAME], queue)

    def test_contextValuePop(self):
        self.context['foo'] = 'bar'
        self.assertEqual(self.context.pop('foo'), 'bar')
        self.assertEqual(self.context.pop('foo', None), None)

    def test_contextValueOverridden(self):
        self.context['foo'] = 'bar'
        self.context['foo'] = 'bar2'
        self.assertEqual(self.context.get('foo'), 'bar2')

    def test_contextKeyMissingReturnsNone(self):
        self.assertEqual(None, self.context.get('unknown-key'))

    def test_contextGeneratesInstanceName(self):
        self.assertTrue(self.context.instanceName.startswith(self.machineName))

    def test_generatedContextNameIsUnique(self):
        instanceName1 = self.context.instanceName
        state = State('foo', None, CountExecuteCalls(), None)
        context2 = FSMContext({state.name : state}, state, queueName='q')
        instanceName2 = context2.instanceName
        self.assertNotEqual(instanceName1, instanceName2)

    def test_datetimeParsedFromInstaneName(self):
        instanceName = self.context.instanceName
        sdate = datetime.datetime.strptime(instanceName.rsplit('-')[-2], '%Y%m%d%H%M%S')
        self.assertEqual(self.context.getInstanceStartTime(), sdate)

    def test_clone(self):
        self.context['foo'] = 'bar'
        clone = self.getContextWithoutSpecialEntries().clone()
        self.assertEqual({'foo': 'bar'}, self.getContextWithoutSpecialEntries())
        self.assertEqual({'foo': 'bar'}, clone)
        self.assertEqual(self.context.instanceName, clone.instanceName)

        self.context['bar'] = 'foo'
        self.assertEqual({'foo': 'bar', 'bar': 'foo'}, self.getContextWithoutSpecialEntries())
        self.assertEqual({'foo': 'bar'}, clone)
        self.assertEqual(self.context.instanceName, clone.instanceName)

    def test_clone_updateData(self):
        self.context['foo'] = 'bar'
        clone = self.getContextWithoutSpecialEntries().clone(updateData={'abc': '123'})
        self.assertEqual({'foo': 'bar', 'abc': '123'}, clone)

    def test_clone_replaceData(self):
        self.context['foo'] = 'bar'
        clone = self.getContextWithoutSpecialEntries().clone(replaceData={'abc': '123'})
        self.assertEqual({'abc': '123'}, clone)

    def test_clone_instanceName(self):
        self.context['foo'] = 'bar'
        clone = self.getContextWithoutSpecialEntries().clone(instanceName='foo')
        self.assertEqual({'foo': 'bar'}, clone)
        self.assertNotEqual(self.context.instanceName, clone.instanceName)

    def test_fork(self):
        self.context.fork()
        self.assertTrue(FORKED_CONTEXTS_PARAM in self.obj)
        self.assertEqual(len(self.obj[FORKED_CONTEXTS_PARAM]), 1)
        self.assertEqual(self.obj[FORKED_CONTEXTS_PARAM][0][FORK_PARAM], 0)
        self.context.fork()
        self.assertEqual(len(self.obj[FORKED_CONTEXTS_PARAM]), 2)
        self.assertEqual(self.obj[FORKED_CONTEXTS_PARAM][0][FORK_PARAM], 0)
        self.assertEqual(self.obj[FORKED_CONTEXTS_PARAM][1][FORK_PARAM], 1)

class FSMContextMergeJoinTests(AppEngineTestCase):

    def setUp(self):
        super().setUp()
        self.state = State('foo', None, CountExecuteCalls(), None)
        self.state2 = State('foo2', None, CountExecuteCallsWithFork(), None)
        self.state.addTransition(Transition('t1', self.state2, queueName='q'), 'event')
        self.context = FSMContext(self.state,
                                  currentState=self.state,
                                  machineName='machineName',
                                  instanceName='instanceName',
                                  queueName='qq',
                                  obj={TASK_NAME_PARAM: 'taskName'})
        self.context.startingState = self.state
        from google.appengine.api.taskqueue.taskqueue import TaskRetryOptions
        self.context.retryOptions = TaskRetryOptions()
        self.context[INDEX_PARAM] = 1
        self.context[STEPS_PARAM] = 0

    def test_mergeJoinDispatch_1_context(self):
        _FantasmFanIn(workIndex='instanceName--foo--event--foo2--step-0-2654435761').put()
        self.assertEqual(1, _FantasmFanIn.all(namespace='').count())
        contexts = self.context.mergeJoinDispatch('event', {RETRY_COUNT_PARAM: 0})
        self.assertEqual([{'__ix__': 1, '__step__': 0}], contexts)
        self.assertEqual(1, _FantasmFanIn.all(namespace='').count())

    def test_mergeJoinDispatch_1234_contexts(self):
        for i in range(1234):
            _FantasmFanIn(workIndex='instanceName--foo--event--foo2--step-0-2654435761').put()
        self.assertEqual(1000, _FantasmFanIn.all(namespace='').count()) # can't get them all with .count()
        contexts = self.context.mergeJoinDispatch('event', {RETRY_COUNT_PARAM: 0})
        self.assertEqual(1234, len(contexts))
        self.assertEqual(1000, _FantasmFanIn.all(namespace='').count())



class TaskQueueFSMTests(AppEngineTestCase):

    def setUp(self):
        super().setUp()
        self.maxDiff = None
        filename = 'test-TaskQueueFSMTests.yaml'
        setUpByFilename(self, filename)
        machineName = getMachineNameByFilename(filename)

        states = self.factory.machines[machineName][MACHINE_STATES_ATTRIBUTE]
        self.stateInitial = states['state-initial']
        self.stateNormal = states['state-normal']
        self.stateFinal = states['state-final']
        self.transInitialToNormal = self.stateInitial._eventToTransition['next-event']
        self.transNormalToFinal = self.stateNormal._eventToTransition['next-event']

    def tearDown(self):
        super().tearDown()
        restore()

    def assertTaskUrlHasStateAndEvent(self, task, expectedState, expectedEvent):
        # '/fantasm/fsm/TaskQueueFSMTests/?__st__=state-normal&__ev__=next-event&arg1=val1&arg2=val2'
        stateParams = '{}={}'.format(STATE_PARAM, expectedState)
        eventParams = '{}={}'.format(EVENT_PARAM, expectedEvent)
        self.assertTrue(stateParams in task.url)
        self.assertTrue(eventParams in task.url)

    def assertTaskUrlHasInstanceName(self, task, instanceName):
        instanceParams = '{}={}'.format(INSTANCE_NAME_PARAM, instanceName)
        self.assertTrue(instanceParams in task.url)

    def test_initialialize_counts(self):
        event = self.context.initialize()
        self.assertEqual('pseudo-init', event)
        self.assertEqual({'state-initial': {'entry': 0, 'action': 0, 'exit': 0},
                          'state-normal': {'entry': 0, 'action': 0, 'exit': 0},
                          'state-final': {'entry': 0, 'action': 0, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-normal--next-event': {'action': 0}},
                         getCounts(self.machineConfig))

    def test_initialDispatchEmitsEventAsTask(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        event = self.context.initialize()
        self.assertEqual(len(mockQueue.tasks), 1)

        self.context.dispatch(event, {})

        self.assertEqual(len(mockQueue.tasks), 2)
        (task, transactional) = mockQueue.tasks[1]

        # state-initial is the state we're transitioning FROM
        self.assertTaskUrlHasStateAndEvent(task, 'state-initial', 'next-event')

    def test_initialDispatch_counts(self):
        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})
        self.assertEqual({'state-initial': {'entry': 0, 'action': 0, 'exit': 1},
                          'state-normal': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-final': {'entry': 0, 'action': 0, 'exit': 0},
                          'state-initial--next-event': {'action': 1},
                          'state-normal--next-event': {'action': 0}},
                         getCounts(self.machineConfig))

    def test_normalStateDispatchWithEventEmitsEventAsTask(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})

        self.assertEqual(len(mockQueue.tasks), 1)
        (task, transactional) = mockQueue.tasks[0]
        # state-normal is the state we're transitioning FROM
        self.assertTaskUrlHasStateAndEvent(task, 'state-normal', 'next-event')

    def test_normalStateDispatchWithEventRespectsCountdown(self):
        import time
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})

        (task, transactional) = mockQueue.tasks[0]
        self.assertTrue(time.time()+20 - getattr(task, '_Task__eta_posix') < 0.01)

    def test_finalStateDispatch_counts(self):
        self.context.currentState = self.stateNormal
        self.context.dispatch('next-event', {})
        self.assertEqual({'state-initial': {'entry': 0, 'action': 0, 'exit': 0},
                          'state-normal': {'entry': 0, 'action': 0, 'exit': 1},
                          'state-final': {'entry': 1, 'action': 1, 'exit': 0},
                          'state-initial--next-event': {'action': 0},
                          'state-normal--next-event': {'action': 1}},
                         getCounts(self.machineConfig))

    def test_finalStateDispatchWithEventEmitsNoEventAsTask(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        self.context.currentState = self.stateNormal
        self.context.dispatch('next-event', {})

        self.assertEqual(len(mockQueue.tasks), 0)

    def test_unknownEventLogsCriticalEvent(self):
        loggingDouble = getLoggingDouble()

        self.context.currentState = self.stateInitial
        self.assertRaises(UnknownEventError, self.context.dispatch, 'bad-event', {})
        # The "bad-event" message is logged twice: once when first looking it up (which raises exception),
        # then again when handling the exception (which uses the event to find the transition to find the retry policy)
        self.assertEqual(loggingDouble.count['critical'], 2)

    def test_nonFinalStateEmittingNoEventLogsCriticalEvent(self):

        def executeReturnsNoEvent(context, obj):
            return None

        mock(name='CountExecuteCalls.execute', returns_func=executeReturnsNoEvent, tracker=None)
        loggingDouble = getLoggingDouble()
        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})
        self.assertEqual(loggingDouble.count['critical'], 1)

    def test_instanceNameIsPropagated(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        event = self.context.initialize()
        self.context.dispatch(event, {})

        self.assertEqual(len(mockQueue.tasks), 2)
        (task, transactional) = mockQueue.tasks[0]
        self.assertTaskUrlHasInstanceName(task, self.context.instanceName)

    def test_getTaskName(self):
        self.context.dispatch(self.context.initialize(), {})
        self.context.instanceName = 'instanceName'
        self.assertEqual('instanceName--state-initial--next-event--state-normal--step-1',
                         self.context.getTaskName('next-event'))

    def test_getTaskName_multiple_steps(self):
        self.context.dispatch(self.context.initialize(), {})
        self.context.instanceName = 'instanceName'
        self.context[STEPS_PARAM] = '123'
        self.assertEqual('instanceName--state-initial--next-event--state-normal--step-123',
                         self.context.getTaskName('next-event'))

    def test_taskQueueOnQueueSpecifiedAtTransitionLevel(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.__init__', returns_func=mockQueue.__init__, tracker=None)
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        self.transNormalToFinal.queueName = 'fantasm-queue' # should be this one (dest state)
        self.transInitialToNormal.queueName = 'barfoo'
        self.context.queueName = 'foobar'

        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})

        self.assertEqual(mockQueue.name, 'fantasm-queue')

    def test_taskTargetSpecifiedAtTransitionLevel(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.__init__', returns_func=mockQueue.__init__, tracker=None)
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        self.transNormalToFinal.taskTarget = 'correct-target' # should be this one (dest state)
        self.transInitialToNormal.taskTarget = 'other-target'
        self.context.globalTaskTarget = 'global-target'

        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})

        self.assertEqual(mockQueue.tasks[0][0].target, 'correct-target')

    def test_setQueueCanAlterTheDispatchQueue(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.__init__', returns_func=mockQueue.__init__, tracker=None)
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)
        self.transNormalToFinal.queueName = 'fantasm-queue' # this is what we'll override
        self.context.currentState = self.stateInitial
        alternateQueue = 'some-other-queue'
        self.context.setQueue(alternateQueue)
        self.context.dispatch('next-event', {})
        self.assertEqual(mockQueue.name, alternateQueue)

    def test_headerCanAlterTheDispatchQueue(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.__init__', returns_func=mockQueue.__init__, tracker=None)
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)
        self.transNormalToFinal.queueName = 'fantasm-queue' # this is what we'll override
        self.context.currentState = self.stateInitial
        alternateQueue = 'some-other-queue'
        self.context.headers[HTTP_REQUEST_HEADER_QUEUENAME] = alternateQueue
        self.context.dispatch('next-event', {})
        self.assertEqual(mockQueue.name, alternateQueue)

    def test_setQueueCanAlterTheDispatchQueueEvenHeadersArePresent(self):
        mockQueue = TaskQueueDouble()
        mock(name='Queue.__init__', returns_func=mockQueue.__init__, tracker=None)
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)
        self.transNormalToFinal.queueName = 'fantasm-queue' # this is what we'll override
        self.context.headers[HTTP_REQUEST_HEADER_QUEUENAME] = 'queueName'
        self.context.currentState = self.stateInitial
        alternateQueue = 'some-other-queue'
        self.context.setQueue(alternateQueue)
        self.context.dispatch('next-event', {})
        self.assertEqual(mockQueue.name, alternateQueue)

    # These tests are not raising as expected. The mock object is not being called. TODO sort this out
    # def test_nextEventNotStringRaisesException(self):
    #     def executeReturnsNoEvent(context, object):
    #         return None
    #
    #     mock(name='CountExecuteCalls.execute', returns_func=executeReturnsNoEvent, tracker=None)
    #     self.context.currentState = self.stateNormal
    #     self.transNormalToFinal.taskRetryLimit = 10
    #     self.context.taskRetryLimit = 10
    #     self.assertRaises(InvalidEventNameRuntimeError, self.context.dispatch, 'next-event', None)
    #
    # def test_invalidNextEventRaisesException(self):
    #     def executeReturnsBadEvent(context, object):
    #         return '*%&#)%&'
    #
    #     mock(name='CountExecuteCalls.execute', returns_func=executeReturnsBadEvent, tracker=None)
    #     self.context.currentState = self.stateNormal
    #     self.transNormalToFinal.taskRetryLimit = 10
    #     self.context.taskRetryLimit = 10
    #     self.assertRaises(InvalidEventNameRuntimeError, self.context.dispatch, 'next-event', None)

class TaskQueueFSMRandomCountdownTests(AppEngineTestCase):

    def setUp(self):
        super().setUp()
        self.maxDiff = None
        filename = 'test-TaskQueueFSMRandomCountdownTests.yaml'
        setUpByFilename(self, filename)
        machineName = getMachineNameByFilename(filename)

        states = self.factory.machines[machineName][MACHINE_STATES_ATTRIBUTE]
        self.stateInitial = states['state-initial']
        self.stateNormal = states['state-normal']
        self.stateFinal = states['state-final']
        self.transInitialToNormal = self.stateInitial._eventToTransition['next-event']
        self.transNormalToFinal = self.stateNormal._eventToTransition['next-event']

    def tearDown(self):
        super().tearDown()
        restore()

    def test_normalStateDispatchUsesRandomCountdown(self):
        import time
        mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=mockQueue.add, tracker=None)

        self.context.currentState = self.stateInitial
        self.context.dispatch('next-event', {})

        (task, transactional) = mockQueue.tasks[0]
        # compare 29->61 instead of 30->60 just to leave a little wiggy room for test execution time
        self.assertTrue(time.time()+29 <= getattr(task, '_Task__eta_posix'))
        self.assertTrue(time.time()+61 >= getattr(task, '_Task__eta_posix'))

# some bits borrowed from the taskqueue implementation
class _UTCTimeZone(datetime.tzinfo):
    """UTC timezone."""
    ZERO = datetime.timedelta(0)
    def utcoffset(self, dt):
        return self.ZERO
    def dst(self, dt):
        return self.ZERO
    def tzname(self, dt):
        return 'UTC'
_UTC = _UTCTimeZone()

class TaskQueueFSMRetryTests(AppEngineTestCase):

    def setUp(self):
        super().setUp()
        filename = 'test-TaskQueueFSMRetryTests.yaml'
        machineName = getMachineNameByFilename(filename)
        self.factory = getFSMFactoryByFilename(filename)
        self.context = self.factory.createFSMInstance(machineName)

        self.mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=self.mockQueue.add, tracker=None)
        self.loggingDouble = getLoggingDouble()

        # drive the machine to ready
        self.initEvent = self.context.initialize()
        self.mockQueue.purge() # clear the initialization task

    def tearDown(self):
        super().tearDown()
        restore()

    def test_taskRetryLimitAddedToQueuedTask(self):
        def execute(context, obj):
            return 'ok1'
        mock(name='CountExecuteCalls.execute', returns_func=execute, tracker=None)
        self.context.dispatch(self.initEvent, TemporaryStateObject())
        self.assertEqual(len(self.mockQueue.tasks), 1)
        task = self.mockQueue.tasks[0][0]
        self.assertEqual(task.retry_options.task_retry_limit, 1)

    def test_minBackoffSecondsAddedToQueuedTask(self):
        def execute(context, obj):
            return 'ok2'
        mock(name='CountExecuteCalls.execute', returns_func=execute, tracker=None)
        self.context.dispatch(self.initEvent, TemporaryStateObject())
        self.assertEqual(len(self.mockQueue.tasks), 1)
        task = self.mockQueue.tasks[0][0]
        self.assertEqual(task.retry_options.min_backoff_seconds, 2)

    def test_maxBackoffSecondsAddedToQueuedTask(self):
        def execute(context, obj):
            return 'ok3'
        mock(name='CountExecuteCalls.execute', returns_func=execute, tracker=None)
        self.context.dispatch(self.initEvent, TemporaryStateObject())
        self.assertEqual(len(self.mockQueue.tasks), 1)
        task = self.mockQueue.tasks[0][0]
        self.assertEqual(task.retry_options.max_backoff_seconds, 3)

    def test_taskAgeLimitAddedToQueuedTask(self):
        def execute(context, obj):
            return 'ok4'
        mock(name='CountExecuteCalls.execute', returns_func=execute, tracker=None)
        self.context.dispatch(self.initEvent, TemporaryStateObject())
        self.assertEqual(len(self.mockQueue.tasks), 1)
        task = self.mockQueue.tasks[0][0]
        self.assertEqual(task.retry_options.task_age_limit, 4)

    def test_maxDoublingsAddedToQueuedTask(self):
        def execute(context, obj):
            return 'ok5'
        mock(name='CountExecuteCalls.execute', returns_func=execute, tracker=None)
        self.context.dispatch(self.initEvent, TemporaryStateObject())
        self.assertEqual(len(self.mockQueue.tasks), 1)
        task = self.mockQueue.tasks[0][0]
        self.assertEqual(task.retry_options.max_doublings, 5)

class TestModel(db.Model):
    prop1 = db.StringProperty()

class DatastoreFSMContinuationBaseTests(AppEngineTestCase):

    FILENAME = None
    MACHINE_NAME = None

    def setUp(self):
        super().setUp()
        setUpByFilename(self, self.FILENAME, instanceName='instanceName', machineName=self.MACHINE_NAME)
        self.mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=self.mockQueue.add, tracker=None)
        self.loggingDouble = getLoggingDouble()
        self.modelKeys = []
        for i in range(10):
            modelKey = TestModel().put()
            self.modelKeys.append(modelKey)

    def tearDown(self):
        super().tearDown()
        restore()

class DatastoreFSMContinuationWithContinuationCountdownTests(DatastoreFSMContinuationBaseTests):

    FILENAME = 'test-DatastoreFSMContinuationTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationCountdownTests'

    def test_DatastoreFSMContinuation_queues_a_continuation_task(self):
        event = self.context.initialize()
        self.assertEqual(1, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(2, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual('instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1',
                         self.mockQueue.tasks[-2][0].name)
        now = time.time()
        expectedEta = now + 30
        self.assertTrue(self.mockQueue.tasks[-2][0].eta_posix - expectedEta < 2)
        self.assertEqual(4, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(4, len(self.mockQueue.tasks))

        self.assertEqual(None, event)

class DatastoreFSMContinuationTests(DatastoreFSMContinuationBaseTests):

    FILENAME = 'test-DatastoreFSMContinuationTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationTests'

    def test_DatastoreFSMContinuation_smoke_test(self):
        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertTrue(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        self.assertTrue(event is not None)

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        self.assertEqual(None, event)

    def test_DatastoreFSMContinuation_continuation_param_is_popped_from_context(self):
        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))

        # test that continuation pops the continuation param out for current machine
        obj = TemporaryStateObject()
        query = db.GqlQuery("SELECT * FROM TestModel ORDER BY prop1")
        query.fetch(5)
        cursor = query.cursor()
        self.context[CONTINUATION_PARAM] = cursor
        event = self.context.dispatch(event, obj)
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertTrue(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM)) # continuation param is popped out
        self.assertEqual(self.modelKeys[5:7], [m.key() for m in obj[CONTINUATION_RESULTS_KEY]])

        # and check that the expected cursor is in the continuation task
        query.with_cursor(cursor) # unexpected - i would have though the previous fetch() would leave the cursor
        query.fetch(2)
        self.assertTrue(urllib.parse.quote(query.cursor()) in self.mockQueue.tasks[-2][0].url)

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        self.assertEqual(None, event)

    def test_DatastoreFSMContinuation_queues_a_continuation_task(self):
        event = self.context.initialize()
        self.assertEqual(1, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(2, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual('instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1',
                         self.mockQueue.tasks[-2][0].name)
        self.assertEqual(4, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(4, len(self.mockQueue.tasks))

        self.assertEqual(None, event)

    def test_DatastoreFSMContinuation_queue_continuation_fails_if_already_queued(self):
        event = self.context.initialize()
        self.assertEqual(1, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(2, len(self.mockQueue.tasks))

        # patch a failing do action
        originalAction = self.context.currentState.getTransition(event).target.doAction
        try:
            self.context.currentState.getTransition(event).target.doAction = RaiseExceptionContinuationAction()
            self.assertRaises(Exception, self.context.dispatch, event, TemporaryStateObject())
            self.assertEqual('state-initial', self.context.currentState.name)
            self.assertEqual(3, len(self.mockQueue.tasks))
            self.assertEqual('instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1',
                             self.mockQueue.tasks[-1][0].name)
        finally:
            self.context.currentState.getTransition(event).target.doAction = originalAction # patch it back

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('Unable to queue continuation Task as it already exists. ' +
                         '(Machine DatastoreFSMContinuationTests, State state-continuation)',
                         self.loggingDouble.messages['info'][-1])
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual(4, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(4, len(self.mockQueue.tasks))

        self.assertEqual(None, event)

class DatastoreFSMContinuationFanInTests(DatastoreFSMContinuationBaseTests):

    FILENAME = 'test-DatastoreFSMContinuationFanInTests.yaml'
    MACHINE_NAME = 'DatastoreFSMContinuationFanInTests'

    def test_DatastoreFSMContinuationFanIn_smoke_test(self):
        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)

        obj = TemporaryStateObject()
        obj[TASK_NAME_PARAM] = 'taskName'
        obj[RETRY_COUNT_PARAM] = 0

        event = self.context.dispatch(event, obj)
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(0, _FantasmFanIn.all(namespace='').count())

        event = self.context.dispatch(event, obj)
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual(1, _FantasmFanIn.all(namespace='').count())

        event = self.context.dispatch(event, obj)
        self.assertEqual('state-fan-in', self.context.currentState.name)
        self.assertEqual(1, _FantasmFanIn.all(namespace='').count())

        event = self.context.dispatch(event, obj)
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(1, _FantasmFanIn.all(namespace='').count())

    def test_DatastoreFSMContinuationFanInTests_write_lock_error(self):
        obj = TemporaryStateObject()
        obj[TASK_NAME_PARAM] = 'taskName'

        event = self.context.initialize() # queues the first task
        self.assertEqual('pseudo-init', self.context.currentState.name)

        event = self.context.dispatch(event, obj)
        self.assertEqual('state-initial', self.context.currentState.name)

        mock('memcache.incr', returns=1, tracker=None)

        # the .dispatch() takes the machine into "state-continuation" and the queueDispatch() looks
        # ahead to determine that _queueDispatchFanIn() is required, which raises the expected error
        # due to lock acquisition problems
        self.assertRaises(FanInWriteLockFailureRuntimeError, self.context.dispatch, event, obj)

#    def test_DatastoreFSMContinuationFanIn_race_on_work_delete(self):
#        event = self.context.initialize()
#        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
#        self.assertFalse(self.context.currentState.isContinuation)
#
#        event = self.context.dispatch(event, TemporaryStateObject())
#        self.assertEqual('state-initial', self.context.currentState.name)
#        self.assertEqual(0, _FantasmFanIn.all(namespace='').count())
#
#        event = self.context.dispatch(event, TemporaryStateObject())
#        self.assertEqual('state-continuation', self.context.currentState.name)
#        self.assertEqual(1, _FantasmFanIn.all(namespace='').count())
#
#        writeLock = '%s-lock-%d' % (self.context.getTaskName(event, fanIn=True), self.context.get(INDEX_PARAM))
#        readLock = '%s-readlock-%d' % (self.context.getTaskName(event, fanIn=True), self.context.get(INDEX_PARAM))
#        def memcacheget(arg):
#            if arg == readLock:
#                return 'not-me'
#            return mocked[1][0](arg)
#        mock('memcache.get', returns_func=memcacheget, tracker=None)
#
#        self.assertRaises(FanInReadLockFailureRuntimeError, self.context.dispatch, event, TemporaryStateObject())
#        self.assertEqual('state-continuation', self.context.currentState.name)

    def test_DatastoreFSMContinuationFanIn_work_packages_restored_on_exception(self):

        obj = TemporaryStateObject()
        obj[TASK_NAME_PARAM] = 'taskName'
        obj[RETRY_COUNT_PARAM] = 0

        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)

        event = self.context.dispatch(event, obj)
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(0, _FantasmFanIn.all(namespace='').count())

        event = self.context.dispatch(event, obj)
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual(1, _FantasmFanIn.all(namespace='').count())

        # override the action of the transition raise an exception
        originalAction = self.context.currentState.getTransition(event).action
        try:
            self.context.currentState.getTransition(event).action = RaiseExceptionAction()
            self.assertRaises(Exception, self.context.dispatch, event, obj)
            self.assertEqual('state-continuation', self.context.currentState.name)
            self.assertEqual(1, _FantasmFanIn.all(namespace='').count()) # the work packages are restored on exception
        finally:
            self.context.currentState.getTransition(event).action = originalAction # and restore

        event = self.context.dispatch(event, obj)
        self.assertEqual('state-fan-in', self.context.currentState.name)
        self.assertEqual(1, _FantasmFanIn.all(namespace='').count())

        event = self.context.dispatch(event, obj)
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(1, _FantasmFanIn.all(namespace='').count())

class NDBTestModel(ndb_model.Model):
    prop1 = ndb_model.StringProperty()

class NDBDatastoreFSMContinuationTests(AppEngineTestCase):

    FILENAME = 'test-NDBDatastoreFSMContinuationTests.yaml'
    MACHINE_NAME = 'NDBDatastoreFSMContinuationTests'

    def setUp(self):
        super().setUp()
        setUpByFilename(self, self.FILENAME, instanceName='instanceName', machineName=self.MACHINE_NAME)
        self.mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=self.mockQueue.add, tracker=None)
        self.loggingDouble = getLoggingDouble()
        self.modelKeys = []
        for i in range(10):
            modelKey = NDBTestModel().put()
            self.modelKeys.append(modelKey)

    def tearDown(self):
        super().tearDown()
        restore()

    def test_NDBDatastoreFSMContinuation_smoke_test(self):
        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertTrue(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        self.assertTrue(event is not None)

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        self.assertEqual(None, event)

    def test_NDBDatastoreFSMContinuation_continuation_param_is_popped_from_context(self):
        event = self.context.initialize()
        self.assertTrue(FSM.PSEUDO_INIT, self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))

        # test that continuation pops the continuation param out for current machine
        obj = TemporaryStateObject()
        query = NDBTestModel.query().order(NDBTestModel.prop1)
        _, cursor, _ = query.fetch_page(5)
        self.context[CONTINUATION_PARAM] = cursor.to_websafe_string()
        event = self.context.dispatch(event, obj)
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertTrue(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM)) # continuation param is popped out
        self.assertEqual(self.modelKeys[5:7], [m.key for m in obj[CONTINUATION_RESULTS_KEY]])

        # and check that the expected cursor is in the continuation task
        _, cursor, _ = query.fetch_page(2, start_cursor=cursor)
        self.assertTrue(urllib.parse.quote(cursor.to_websafe_string()) in self.mockQueue.tasks[-2][0].url)

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertFalse(self.context.currentState.isContinuation)
        self.assertFalse(self.context.get(CONTINUATION_PARAM))
        self.assertEqual(None, event)

    def test_NDBDatastoreFSMContinuation_queues_a_continuation_task(self):
        event = self.context.initialize()
        self.assertEqual(1, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(2, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual('instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1',
                         self.mockQueue.tasks[-2][0].name)
        self.assertEqual(4, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(4, len(self.mockQueue.tasks))

        self.assertEqual(None, event)

    def test_NDBDatastoreFSMContinuation_queue_continuation_fails_if_already_queued(self):
        event = self.context.initialize()
        self.assertEqual(1, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        self.assertEqual(2, len(self.mockQueue.tasks))

        # patch a failing do action
        originalAction = self.context.currentState.getTransition(event).target.doAction
        try:
            self.context.currentState.getTransition(event).target.doAction = RaiseExceptionContinuationAction()
            self.assertRaises(Exception, self.context.dispatch, event, TemporaryStateObject())
            self.assertEqual('state-initial', self.context.currentState.name)
            self.assertEqual(3, len(self.mockQueue.tasks))
            self.assertEqual('instanceName--continuation-1-1--state-initial--next-event--state-continuation--step-1',
                             self.mockQueue.tasks[-1][0].name)
        finally:
            self.context.currentState.getTransition(event).target.doAction = originalAction # patch it back

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('Unable to queue continuation Task as it already exists. ' +
                         '(Machine NDBDatastoreFSMContinuationTests, State state-continuation)',
                         self.loggingDouble.messages['info'][-1])
        self.assertEqual('state-continuation', self.context.currentState.name)
        self.assertEqual(4, len(self.mockQueue.tasks))

        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-final', self.context.currentState.name)
        self.assertEqual(4, len(self.mockQueue.tasks))

        self.assertEqual(None, event)

class ContextTypesCoercionTests(unittest.TestCase):

    def setUp(self):
        super().setUp()
        setUpByFilename(self, 'test-TypeCoercionTests.yaml')

    def test_incomingItemsArePlacedIntoContextAsCorrectDatatype(self):
        nkey = ndb_key.Key('MyModel', '123')
        dt = datetime.datetime(2010, 9, 7, 1, 31, 10)
        self.context.putTypedValue('counter', '123')
        self.context.putTypedValue('batch-key', 'agxmYW50YXNtLXRlc3RyEAsSCkVtYWlsQmF0Y2gYUAw')
        self.context.putTypedValue('data', json.dumps({'a': 'a'}))
        self.context.putTypedValue('start-date', pickle.dumps(dt))
        self.context.putTypedValue('ndb_key_key', nkey.urlsafe())
        self.context.putTypedValue('ndb_model_key', nkey.urlsafe())
        self.context.putTypedValue('ndb_context_key', nkey.urlsafe())
        self.assertEqual(self.context['counter'], 123)
        self.assertTrue(isinstance(self.context['batch-key'], db.Key))
        self.assertEqual({'a': 'a'}, self.context['data'])
        self.assertEqual(dt, self.context['start-date'])
        self.assertEqual(self.context['ndb_key_key'], nkey)
        self.assertEqual(self.context['ndb_model_key'], nkey)
        self.assertEqual(self.context['ndb_context_key'], nkey)

    def test_internalParametersArePlacedIntoContextAsCorrectDatatype(self):
        self.context.putTypedValue(STEPS_PARAM, '123')
        self.assertEqual(self.context[STEPS_PARAM], 123)

        self.context.putTypedValue(GEN_PARAM, '{"123": 123}')
        self.assertEqual(self.context[GEN_PARAM], {'123': 123})

        self.context.putTypedValue(INDEX_PARAM, '123')
        self.assertEqual(self.context[INDEX_PARAM], 123)

class ContextYamlImportTests(unittest.TestCase):

    def setUp(self):
        super().setUp()

    def test_imports_only(self):
        setUpByFilename(self, 'test-YamlImportOnly.yaml', machineName='TypeCoercionTests')
        self.assertTrue('MyMachine' in self.currentConfig.machines)
        self.assertTrue('TypeCoercionTests' in self.currentConfig.machines)

    def test_imports_and_machines(self):
        setUpByFilename(self, 'test-YamlImport.yaml', machineName='Foo')
        self.assertTrue('MyMachine' in self.currentConfig.machines)
        self.assertTrue('TypeCoercionTests' in self.currentConfig.machines)
        self.assertTrue('Foo' in self.currentConfig.machines)

    def test_import_does_not_change_global_config_root_url(self):
        setUpByFilename(self, 'test-YamlImport.yaml', machineName='Foo')
        self.assertEqual('/another-root/', self.currentConfig.rootUrl)

    def test_import_does_not_change_global_config_enable_capabilities_check(self):
        setUpByFilename(self, 'test-YamlImport.yaml', machineName='Foo')
        self.assertFalse(self.currentConfig.enableCapabilitiesCheck)

    def test_import_circular_fails(self):
        self.assertRaises(YamlFileCircularImportError, setUpByFilename,
                          self, 'test-YamlImportCircular.yaml', machineName='Foo')

class SpawnTests(unittest.TestCase):

    def setUp(self):
        super().setUp()
        filename = 'test-TaskQueueFSMTests.yaml'
        setUpByFilename(self, filename)
        self.machineName = getMachineNameByFilename(filename)
        self.mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=self.mockQueue.add, tracker=None)
        # dispatch initial event to get context in correct state
        self.taskName = 'foo'
        self.obj = {TASK_NAME_PARAM: self.taskName}
        self.context.dispatch(FSM.PSEUDO_INIT, self.obj)
        # but now flush the task queue to remove the event that we just dispatched;
        # we're only interested in testing spawn tasks
        self.mockQueue.purge()

    def tearDown(self):
        super().tearDown()
        restore()

    def getTask(self, num):
        """ Retrieves a queued task from mock queue. """
        return self.mockQueue.tasks[num][0]

    def test_spawnWithNoContextDoesNotQueueAnything(self):
        self.context.spawn(self.machineName, None, _currentConfig=self.currentConfig)
        self.assertEqual(len(self.mockQueue.tasks), 0)

    def test_spawnWithOneContextQueuesOne(self):
        self.context.spawn(self.machineName, {'a': '1'}, _currentConfig=self.currentConfig)
        self.assertEqual(len(self.mockQueue.tasks), 1)

    def test_spawnWithTwoContextsQueuesTwo(self):
        self.context.spawn(self.machineName, [{'a': '1'}, {'b': '2'}], _currentConfig=self.currentConfig)
        self.assertEqual(len(self.mockQueue.tasks), 2)

    def test_spawnUsesCorrectUrl(self):
        self.context.spawn(self.machineName, [{'a': '1'}, {'b': '2'}], _currentConfig=self.currentConfig)
        self.assertTrue(self.getTask(0).url.startswith('/fantasm/fsm/%s/' % self.machineName))
        self.assertTrue(self.getTask(1).url.startswith('/fantasm/fsm/%s/' % self.machineName))

    def test_contextAreIncludedInTasks(self):
        self.context.spawn(self.machineName, [{'a': '1'}, {'b': '2'}], _currentConfig=self.currentConfig,
                           method='GET')
        self.assertTrue('a=1' in self.getTask(0).url)
        self.assertTrue('b=2' in self.getTask(1).url)

    def test_countdownIsIncludedInTask(self):
        # having trouble mocking Task, so I'll dip into a private attribute right on task
        import time
        self.context.spawn(self.machineName, {'a': '1'}, countdown=20, _currentConfig=self.currentConfig)
        self.assertTrue(time.time()+20 - getattr(self.mockQueue.tasks[0][0], '_Task__eta_posix') < 0.01)

    def test_spawnIsIdempotent(self):
        self.context.spawn(self.machineName, {'a': '1'}, _currentConfig=self.currentConfig)
        self.assertEqual(len(self.mockQueue.tasks), 1)
        self.context.spawn(self.machineName, {'a': '1'}, _currentConfig=self.currentConfig)
        self.assertEqual(len(self.mockQueue.tasks), 1)

class StartStateMachineTests(unittest.TestCase):
    """ Tests for startStateMachine """

    def setUp(self):
        super().setUp()
        filename = 'test-TaskQueueFSMTests.yaml'
        setUpByFilename(self, filename)
        self.machineName = getMachineNameByFilename(filename)
        self.mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=self.mockQueue.add, tracker=None)

    def tearDown(self):
        super().tearDown()
        restore()

    def getTask(self, num):
        """ Retrieves a queued task from mock queue. """
        return self.mockQueue.tasks[num][0]

    def test_taskEnqueuedToStartSingleMachine(self):
        startStateMachine(self.machineName, {'a': '1'}, _currentConfig=self.currentConfig)
        self.assertEqual(len(self.mockQueue.tasks), 1)

    def test_tasksEnqueuedToStartMultipleMachines(self):
        startStateMachine(self.machineName, [{'a': '1'}, {'b': '2'}, {'c': '3'}], _currentConfig=self.currentConfig)
        self.assertEqual(len(self.mockQueue.tasks), 3)

    def test_enqueuedTaskPointsAtCorrectTarget(self):
        startStateMachine(self.machineName, [{'a': '1'}, {'b': '2'}, {'c': '3'}], _currentConfig=self.currentConfig)
        task = self.getTask(1)
        self.assertEqual('backend1', task.target)

    def test_contextsAddedToTasks(self):
        startStateMachine(self.machineName, [{'a': '1'}, {'b': '2'}], _currentConfig=self.currentConfig,
                          method='GET')
        self.assertTrue('a=1' in self.getTask(0).url)
        self.assertTrue('b=2' in self.getTask(1).url)

    def test_correctMethodUsedToEnqueueTask(self):
        startStateMachine(self.machineName, {'a': '1'}, _currentConfig=self.currentConfig, method='GET')
        self.assertEqual(self.getTask(0).method, 'GET')

    def test_correctUrlInTask(self):
        startStateMachine(self.machineName, {'a': '1'}, _currentConfig=self.currentConfig, method='POST')
        self.assertEqual(self.getTask(0).url, '/fantasm/fsm/{}/{}/{}/{}/'.format(self.machineName,
                                                                              FSM.PSEUDO_INIT,
                                                                              FSM.PSEUDO_INIT,
                                                                              self.initialState.name))

    def test_countdownIncludedInTask(self):
        # having trouble mocking Task, so I'll dip into a private attribute right on task
        import time
        startStateMachine(self.machineName, {'a': '1'}, countdown=20, _currentConfig=self.currentConfig)
        self.assertTrue(time.time()+20 - getattr(self.mockQueue.tasks[0][0], '_Task__eta_posix') < 0.01)

    def test_taskNameIsUsedWhenQueuingTasks(self):
        startStateMachine(self.machineName, {'a': '1'}, _currentConfig=self.currentConfig, taskName='foo')
        self.assertTrue(self.getTask(0).name.startswith('foo'))

    def test_uniqueTaskNamesGeneratedForMultipleContexts(self):
        startStateMachine(self.machineName, [{'a': '1'}, {'b': '2'}], _currentConfig=self.currentConfig,
                          taskName='foo')
        self.assertTrue(self.getTask(0).name.startswith('foo'))
        self.assertTrue(self.getTask(1).name.startswith('foo'))
        self.assertTrue(self.getTask(0).name.endswith('0'))
        self.assertTrue(self.getTask(1).name.endswith('1'))

    def test_uniqueTaskNameGeneratedForMultipleContextsWhenTaskNameNotProvided(self):
        startStateMachine(self.machineName, [{'a': '1'}, {'b': '2'}], _currentConfig=self.currentConfig)
        self.assertNotEqual(self.getTask(0).name, self.getTask(1).name)

    def test_startStateMachineIsIdempotent(self):
        startStateMachine(self.machineName, {'a': '1'}, _currentConfig=self.currentConfig, taskName='foo')
        self.assertEqual(len(self.mockQueue.tasks), 1)
        startStateMachine(self.machineName, {'a': '1'}, _currentConfig=self.currentConfig, taskName='foo')
        self.assertEqual(len(self.mockQueue.tasks), 1)

    def test_tasksQueuedForStartStateMachineWithTaskName(self):
        startStateMachine(self.machineName, [{'a': '1'}, {'b': '2'}], _currentConfig=self.currentConfig,
                          taskName='foo')
        self.assertEqual(len(self.mockQueue.tasks), 2)
        self.assertNotEqual(self.getTask(0).name, self.getTask(1).name)

    def test_tasksQueuedForStartStateMachineWithNoTaskName(self):
        startStateMachine(self.machineName, [{'a': '1'}, {'b': '2'}], _currentConfig=self.currentConfig)
        self.assertEqual(len(self.mockQueue.tasks), 2)
        self.assertNotEqual(self.getTask(0).name, self.getTask(1).name)

    def test_raiseIfTaskExists_True(self):
        ld = getLoggingDouble()
        startStateMachine(self.machineName, [{'a': '1'}, {'b': '2'}], taskName='a', _currentConfig=self.currentConfig)
        startStateMachine(self.machineName, [{'a': '1'}, {'b': '2'}], taskName='a', _currentConfig=self.currentConfig)
        self.assertEqual(['Unable to queue new machine TaskQueueFSMTests with taskName a as it has been previously enqueued.'],
                         ld.messages['info'])
        from google.appengine.api.taskqueue.taskqueue import TaskAlreadyExistsError
        self.assertRaises(TaskAlreadyExistsError, startStateMachine, self.machineName, [{'a': '1'}, {'b': '2'}],
                          taskName='a', _currentConfig=self.currentConfig, raiseIfTaskExists=True)
        self.assertEqual(['Unable to queue new machine TaskQueueFSMTests with taskName a as it has been previously enqueued.',
                          'Unable to queue new machine TaskQueueFSMTests with taskName a as it has been previously enqueued.'],
                         ld.messages['info'])
        restore()

    def test_transactionalStartStateMachineTasksNotEmittedIfTransactionFails(self):
        def tx():
            startStateMachine(self.machineName, [{'a': '1'}, {'b': '2'}], transactional=True,
                              _currentConfig=self.currentConfig)
            raise Exception('instrumented exception')
        try:
            db.run_in_transaction(tx)
            self.fail('exception should be raised.')
        except Exception:
            pass # this is expected
        self.assertEqual(len(self.mockQueue.tasks), 0) # nothing should be queued

    def test_queuedInitializationTaskContainsAlternateQueueHeader(self):
        alternateQueue = 'some-queue'
        startStateMachine(self.machineName, {'a': '1'}, queueName=alternateQueue, _currentConfig=self.currentConfig)
        self.assertEqual(len(self.mockQueue.tasks), 1)
        self.assertEqual(self.getTask(0).headers[HTTP_REQUEST_HEADER_QUEUENAME], alternateQueue)


class HaltMachineErrorTest(AppEngineTestCase):

    FILENAME = None
    MACHINE_NAME = None

    def setUp(self):
        super().setUp()
        setUpByFilename(self, self.FILENAME, instanceName='instanceName', machineName=self.MACHINE_NAME)
        self.mockQueue = TaskQueueDouble()
        mock(name='Queue.add', returns_func=self.mockQueue.add, tracker=None)
        self.loggingDouble = getLoggingDouble()

    def tearDown(self):
        super().tearDown()
        restore()

class HaltMachineErrorEntryTests(HaltMachineErrorTest):
    """ Tests for raising HaltMachineError in state's entry. """
    FILENAME = 'test-HaltMachineErrorTests.yaml'
    MACHINE_NAME = 'HaltMachineInEntry'

    def test_error_in_entry_halts_machine(self):
        event = self.context.initialize()
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertIsNone(event) # machine stopping
        self.assertEqual('state-initial', self.context.currentState.name)

class HaltMachineErrorExitTests(HaltMachineErrorTest):
    """ Tests for raising HaltMachineError in state's exit. """
    FILENAME = 'test-HaltMachineErrorTests.yaml'
    MACHINE_NAME = 'HaltMachineInExit'

    def test_error_in_exit_halts_machine(self):
        event = self.context.initialize()
         # "advance" to state-intermediate since the exit state is actually executed before the next state
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertIsNone(event) # machine stopping
        self.assertEqual('state-initial', self.context.currentState.name) # didn't successfully transition

class HaltMachineErrorTransitionTests(HaltMachineErrorTest):
    """ Tests for raising HaltMachineError in transition's action. """
    FILENAME = 'test-HaltMachineErrorTests.yaml'
    MACHINE_NAME = 'HaltMachineInTransition'

    def test_error_in_transition_halts_machine(self):
        event = self.context.initialize()
         # "advance" to state-intermediate since the transition action is actually executed before the next state
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual('state-initial', self.context.currentState.name)
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertIsNone(event) # machine stopping
        self.assertEqual('state-initial', self.context.currentState.name) # didn't successfully transition

class HaltMachineErrorActionTests(HaltMachineErrorTest):
    """ Tests for raising HaltMachineError in state's action. """
    FILENAME = 'test-HaltMachineErrorTests.yaml'
    MACHINE_NAME = 'HaltMachineInAction'

    def test_error_in_action_halts_machine(self):
        event = self.context.initialize()
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertIsNone(event) # machine stopping
        self.assertEqual('state-initial', self.context.currentState.name)

    def test_message_logged_at_appropriate_level(self):
        event = self.context.initialize()
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual(self.loggingDouble.count['debug'], 1)
        self.assertEqual(self.loggingDouble.messages['debug'][0], 'instrumented exception')

class HaltMachineErrorContinuationTests(HaltMachineErrorTest):
    """ Tests for raising HaltMachineError in state's continuation. """
    FILENAME = 'test-HaltMachineErrorTests.yaml'
    MACHINE_NAME = 'HaltInContinuation'

    def test_error_in_continuation_halts_machine(self):
        event = self.context.initialize()
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertIsNone(event) # machine stopping
        self.assertEqual('state-initial', self.context.currentState.name)

class HaltMachineErrorActionNoMessageEmittedTests(HaltMachineErrorTest):
    """ Tests for raising HaltMachineError in state's action. """
    FILENAME = 'test-HaltMachineErrorTests.yaml'
    MACHINE_NAME = 'HaltMachineInActionNoMessage'

    def test_message_logged_at_appropriate_level(self):
        event = self.context.initialize()
        event = self.context.dispatch(event, TemporaryStateObject())
        self.assertEqual(self.loggingDouble.count['debug'], 0)
        self.assertEqual(self.loggingDouble.count['info'], 0)
        self.assertEqual(self.loggingDouble.count['warning'], 0)
        self.assertEqual(self.loggingDouble.count['error'], 0)
        self.assertEqual(self.loggingDouble.count['critical'], 0)
