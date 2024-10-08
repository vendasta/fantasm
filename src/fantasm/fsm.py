""" Fantasm: A taskqueue-based Finite State Machine for App Engine Python

Docs and examples: http://code.google.com/p/fantasm/

Copyright 2010 VendAsta Technologies Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.



The FSM implementation is inspired by the paper:

[1] J. van Gurp, J. Bosch, "On the Implementation of Finite State Machines", in Proceedings of the 3rd Annual IASTED
    International Conference Software Engineering and Applications,IASTED/Acta Press, Anaheim, CA, pp. 172-178, 1999.
    (www.jillesvangurp.com/static/fsm-sea99.pdf)

The Fan-out / Fan-in implementation is modeled after the presentation:

[2] B. Slatkin, "Building high-throughput data pipelines with Google App Engine", Google IO 2010.
    http://code.google.com/events/io/2010/sessions/high-throughput-data-pipelines-appengine.html
"""

import base64
import copy
import datetime
import json
import pickle
import random
import time

from google.appengine.api.taskqueue.taskqueue import (Task,
                                                      TaskAlreadyExistsError,
                                                      TaskRetryOptions,
                                                      TombstonedTaskError)
from google.appengine.ext import db

from fantasm import config, constants, models
from fantasm.exceptions import (TRANSIENT_ERRORS, HaltMachineError,
                                UnknownEventError, UnknownMachineError,
                                UnknownStateError)
from fantasm.lock import ReadWriteLock, RunOnceSemaphore
from fantasm.log import Logger
from fantasm.models import _FantasmFanIn, _FantasmInstance
from fantasm.state import State
from fantasm.transition import Transition
from fantasm.utils import knuthHash


class FSM:
    """ An FSMContext creation factory. This is primarily responsible for translating machine
    configuration information (config.currentConfiguration()) into singleton States and Transitions as per [1]
    """

    PSEUDO_INIT = 'pseudo-init'
    PSEUDO_FINAL = 'pseudo-final'

    _CURRENT_CONFIG = None
    _MACHINES = None
    _PSEUDO_INITS = None
    _PSEUDO_FINALS = None

    def __init__(self, currentConfig=None):
        """ Constructor which either initializes the module/class-level cache, or simply uses it

        @param currentConfig: a config._Configuration instance (dependency injection). if None,
            then the factory uses config.currentConfiguration()
        """
        currentConfig = currentConfig or config.currentConfiguration()

        # if the FSM is not using the currentConfig (.yaml was edited etc.)
        if not (FSM._CURRENT_CONFIG is currentConfig):
            self._init(currentConfig=currentConfig)
            FSM._CURRENT_CONFIG = self.config
            FSM._MACHINES = self.machines
            FSM._PSEUDO_INITS = self.pseudoInits
            FSM._PSEUDO_FINALS = self.pseudoFinals

        # otherwise simply use the cached currentConfig etc.
        else:
            self.config = FSM._CURRENT_CONFIG
            self.machines = FSM._MACHINES
            self.pseudoInits = FSM._PSEUDO_INITS
            self.pseudoFinals = FSM._PSEUDO_FINALS

    def _init(self, currentConfig=None):
        """ Constructs a group of singleton States and Transitions from the machineConfig

        @param currentConfig: a config._Configuration instance (dependency injection). if None,
            then the factory uses config.currentConfiguration()
        """
        import logging
        logging.info("Initializing FSM factory.")

        self.config = currentConfig or config.currentConfiguration()
        self.machines = {}
        self.pseudoInits, self.pseudoFinals = {}, {}
        for machineConfig in list(self.config.machines.values()):
            self.machines[machineConfig.name] = {constants.MACHINE_STATES_ATTRIBUTE: {},
                                                 constants.MACHINE_TRANSITIONS_ATTRIBUTE: {}}
            machine = self.machines[machineConfig.name]

            # create a pseudo-init state for each machine that transitions to the initialState
            pseudoInit = State(FSM.PSEUDO_INIT, None, None, None)
            self.pseudoInits[machineConfig.name] = pseudoInit
            self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][FSM.PSEUDO_INIT] = pseudoInit

            # create a pseudo-final state for each machine that transitions from the finalState(s)
            pseudoFinal = State(FSM.PSEUDO_FINAL, None, None, None, isFinalState=True)
            self.pseudoFinals[machineConfig.name] = pseudoFinal
            self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][FSM.PSEUDO_FINAL] = pseudoFinal

            for stateConfig in list(machineConfig.states.values()):
                state = self._getState(machineConfig, stateConfig)

                # add the transition from pseudo-init to initialState
                if state.isInitialState:
                    transition = Transition(FSM.PSEUDO_INIT, state,
                                            retryOptions = self._buildRetryOptions(machineConfig),
                                            queueName=machineConfig.queueName)
                    self.pseudoInits[machineConfig.name].addTransition(transition, FSM.PSEUDO_INIT)

                # add the transition from finalState to pseudo-final
                if state.isFinalState:
                    transition = Transition(FSM.PSEUDO_FINAL, pseudoFinal,
                                            retryOptions = self._buildRetryOptions(machineConfig),
                                            queueName=machineConfig.queueName)
                    state.addTransition(transition, FSM.PSEUDO_FINAL)

                machine[constants.MACHINE_STATES_ATTRIBUTE][stateConfig.name] = state

            for transitionConfig in list(machineConfig.transitions.values()):
                source = machine[constants.MACHINE_STATES_ATTRIBUTE][transitionConfig.fromState.name]
                transition = self._getTransition(machineConfig, transitionConfig)
                machine[constants.MACHINE_TRANSITIONS_ATTRIBUTE][transitionConfig.name] = transition
                event = transitionConfig.event
                source.addTransition(transition, event)

    def _buildRetryOptions(self, obj):
        """ Builds a TaskRetryOptions object. """
        return TaskRetryOptions(
            task_retry_limit = obj.taskRetryLimit,
            min_backoff_seconds = obj.minBackoffSeconds,
            max_backoff_seconds = obj.maxBackoffSeconds,
            task_age_limit = obj.taskAgeLimit,
            max_doublings = obj.maxDoublings)

    def _getState(self, machineConfig, stateConfig):
        """ Returns a State instance based on the machineConfig/stateConfig

        @param machineConfig: a config._MachineConfig instance
        @param stateConfig: a config._StateConfig instance
        @return: a State instance which is a singleton wrt. the FSM instance
        """

        if machineConfig.name in self.machines and \
           stateConfig.name in self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE]:
            return self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][stateConfig.name]

        name = stateConfig.name
        entryAction = stateConfig.entry
        doAction = stateConfig.action
        exitAction = stateConfig.exit
        isInitialState = stateConfig.initial
        isFinalState = stateConfig.final
        isContinuation = stateConfig.continuation
        continuationCountdown = stateConfig.continuationCountdown
        fanInPeriod = stateConfig.fanInPeriod
        fanInGroup = stateConfig.fanInGroup

        return State(name,
                     entryAction,
                     doAction,
                     exitAction,
                     machineName=machineConfig.name,
                     isInitialState=isInitialState,
                     isFinalState=isFinalState,
                     isContinuation=isContinuation,
                     fanInPeriod=fanInPeriod,
                     fanInGroup=fanInGroup,
                     continuationCountdown=continuationCountdown)

    def _getTransition(self, machineConfig, transitionConfig):
        """ Returns a Transition instance based on the machineConfig/transitionConfig

        @param machineConfig: a config._MachineConfig instance
        @param transitionConfig: a config._TransitionConfig instance
        @return: a Transition instance which is a singleton wrt. the FSM instance
        """
        if machineConfig.name in self.machines and \
           transitionConfig.name in self.machines[machineConfig.name][constants.MACHINE_TRANSITIONS_ATTRIBUTE]:
            return self.machines[machineConfig.name][constants.MACHINE_TRANSITIONS_ATTRIBUTE][transitionConfig.name]

        target = self.machines[machineConfig.name][constants.MACHINE_STATES_ATTRIBUTE][transitionConfig.toState.name]
        retryOptions = self._buildRetryOptions(transitionConfig)
        countdown = transitionConfig.countdown
        queueName = transitionConfig.queueName
        taskTarget = transitionConfig.target

        return Transition(transitionConfig.name, target, action=transitionConfig.action,
                          countdown=countdown, retryOptions=retryOptions, queueName=queueName, taskTarget=taskTarget)

    def createFSMInstance(self, machineName, currentStateName=None, instanceName=None, data=None, method='GET',
                          obj=None, headers=None):
        """ Creates an FSMContext instance with non-initialized data

        @param machineName: the name of FSMContext to instantiate, as defined in fsm.yaml
        @param currentStateName: the name of the state to place the FSMContext into
        @param instanceName: the name of the current instance
        @param data: a dict or FSMContext
        @param method: 'GET' or 'POST'
        @param obj: an object that the FSMContext can operate on
        @param headers: a dict of X-Fantasm request headers to pass along in Tasks
        @raise UnknownMachineError: if machineName is unknown
        @raise UnknownStateError: is currentState name is not None and unknown in machine with name machineName
        @return: an FSMContext instance
        """

        try:
            machineConfig = self.config.machines[machineName]
        except KeyError:
            raise UnknownMachineError(machineName)

        initialState = self.machines[machineName][constants.MACHINE_STATES_ATTRIBUTE][machineConfig.initialState.name]

        try:
            currentState = self.pseudoInits[machineName]
            if currentStateName:
                currentState = self.machines[machineName][constants.MACHINE_STATES_ATTRIBUTE][currentStateName]
        except KeyError:
            raise UnknownStateError(machineName, currentStateName)

        retryOptions = self._buildRetryOptions(machineConfig)
        url = machineConfig.url
        queueName = machineConfig.queueName
        taskTarget = machineConfig.target
        useRunOnceSemaphore = machineConfig.useRunOnceSemaphore

        return FSMContext(initialState, currentState=currentState,
                          machineName=machineName, instanceName=instanceName,
                          retryOptions=retryOptions, url=url, queueName=queueName,
                          data=data, contextTypes=machineConfig.contextTypes,
                          method=method,
                          persistentLogging=(machineConfig.logging == constants.LOGGING_PERSISTENT),
                          obj=obj,
                          headers=headers,
                          globalTaskTarget=taskTarget,
                          useRunOnceSemaphore=useRunOnceSemaphore)

class FSMContext(dict):
    """ A finite state machine context instance. """

    def __init__(self, initialState, currentState=None, machineName=None, instanceName=None,
                 retryOptions=None, url=None, queueName=None, data=None, contextTypes=None,
                 method='GET', persistentLogging=False, obj=None, headers=None, globalTaskTarget=None,
                 useRunOnceSemaphore=True):
        """ Constructor

        @param initialState: a State instance
        @param currentState: a State instance
        @param machineName: the name of the fsm
        @param instanceName: the instance name of the fsm
        @param retryOptions: the TaskRetryOptions for the machine
        @param url: the url of the fsm
        @param queueName: the name of the appengine task queue
        @param headers: a dict of X-Fantasm request headers to pass along in Tasks
        @param persistentLogging: if True, use persistent _FantasmLog model
        @param obj: an object that the FSMContext can operate on
        @param globalTaskTarget: the machine-level target configuration parameter
        """
        assert queueName

        super().__init__(data or {})
        self.initialState = initialState
        self.currentState = currentState
        self.currentAction = None
        if currentState:
            self.currentAction = currentState.exitAction
        self.machineName = machineName
        self.instanceName = instanceName or self._generateUniqueInstanceName()
        self.queueName = queueName
        self.retryOptions = retryOptions
        self.url = url
        self.method = method
        self.startingEvent = None
        self.startingState = None
        self.contextTypes = constants.PARAM_TYPES.copy()
        if contextTypes:
            self.contextTypes.update(contextTypes)
        self.logger = Logger(self, obj=obj, persistentLogging=persistentLogging)
        self.__obj = obj
        self.headers = headers
        self.globalTaskTarget = globalTaskTarget
        self.useRunOnceSemaphore = useRunOnceSemaphore

        # the following is monkey-patched from handler.py for 'immediate mode'
        from google.appengine.api.taskqueue.taskqueue import Queue
        self.Queue = Queue # pylint: disable=C0103

    INSTANCE_NAME_DTFORMAT = '%Y%m%d%H%M%S'

    def _generateUniqueInstanceName(self):
        """ Generates a unique instance name for this machine.

        @return: a FSMContext instanceName that is (pretty darn likely to be) unique
        """
        utcnow = datetime.datetime.utcnow()
        dateStr = utcnow.strftime(self.INSTANCE_NAME_DTFORMAT)
        randomStr = ''.join(random.sample(constants.CHARS_FOR_RANDOM, 6))
        # note this construction is important for getInstanceStartTime()
        return '{}-{}-{}'.format(self.machineName, dateStr, randomStr)

    def getInstanceStartTime(self):
        """ Returns the UTC datetime when the instance was started.

        @return a UTC datetime representing when the instance was started.
        """
        startDatetimeString = self.instanceName.rsplit('-')[-2]
        startDatetime = datetime.datetime.strptime(startDatetimeString, self.INSTANCE_NAME_DTFORMAT)
        return startDatetime

    def putTypedValue(self, key, value):
        """ Sets a value on context[key], but casts the value according to self.contextTypes. """

        # cast the value to the appropriate type TODO: should this be in FSMContext?
        cast = self.contextTypes[key]
        kwargs = {}
        if cast is json.loads:
            kwargs = {'object_hook': models.decode}
        if cast is pickle.loads:
            if isinstance(value, str):
                value = value.encode()
            value = base64.urlsafe_b64decode(value)
            value = pickle.loads(value)
        elif isinstance(value, list):
            value = [cast(v, **kwargs) for v in value]
        else:
            value = cast(value, **kwargs)

        # update the context
        self[key] = value

    def generateInitializationTask(self, countdown=0, taskName=None, transactional=False):
        """ Generates a task for initializing the machine. """
        assert self.currentState.name == FSM.PSEUDO_INIT

        url = self.buildUrl(self.currentState, FSM.PSEUDO_INIT)
        params = self.buildParams(self.currentState, FSM.PSEUDO_INIT)
        if transactional:
            taskName = None
        else:
            taskName = taskName or self.getTaskName(FSM.PSEUDO_INIT)
        transition = self.currentState.getTransition(FSM.PSEUDO_INIT)
        task = Task(name=taskName,
                    method=self.method,
                    url=url,
                    params=params,
                    countdown=countdown,
                    headers=self.headers,
                    retry_options=transition.retryOptions,
                    target=self.globalTaskTarget)
        return task

    def fork(self, data=None):
        """ Forks the FSMContext.

        When an FSMContext is forked, an identical copy of the finite state machine is generated
        that will have the same event dispatched to it as the machine that called .fork(). The data
        parameter is useful for allowing each forked instance to operate on a different bit of data.

        @param data: an option mapping of data to apply to the forked FSMContext
        """
        obj = self.__obj
        if obj.get(constants.FORKED_CONTEXTS_PARAM) is None:
            obj[constants.FORKED_CONTEXTS_PARAM] = []
        forkedContexts = obj.get(constants.FORKED_CONTEXTS_PARAM)
        data = copy.copy(data) or {}
        data[constants.FORK_PARAM] = len(forkedContexts)
        forkedContexts.append(self.clone(updateData=data))

    def spawn(self, machineName, contexts, countdown=0, method='POST',
              _currentConfig=None, taskName=None):
        """ Spawns new machines.

        @param machineName the machine to spawn
        @param contexts a list of contexts (dictionaries) to spawn the new machine(s) with; multiple contexts will spawn
                        multiple machines
        @param countdown the countdown (in seconds) to wait before spawning machines
        @param method the method ('GET' or 'POST') to invoke the machine with (default: POST)
        @param _currentConfig test injection for configuration
        @param taskName used for idempotency; will become the root of the task name for the actual task queued
        """
        # using the current task name as a root to startStateMachine will make this idempotent
        taskName = taskName or self.__obj[constants.TASK_NAME_PARAM]
        startStateMachine(machineName, contexts, taskName=taskName, method=method, countdown=countdown,
                          _currentConfig=_currentConfig, headers=self.headers)

    def initialize(self):
        """ Initializes the FSMContext. Queues a Task (so that we can benefit from auto-retry) to dispatch
        an event and take the machine from 'pseudo-init' into the state machine's initial state, as
        defined in the fsm.yaml file.

        @param data: a dict of initial key, value pairs to stuff into the FSMContext
        @return: an event string to dispatch to the FSMContext to put it into the initialState
        """
        self[constants.STEPS_PARAM] = 0
        task = self.generateInitializationTask()
        self.Queue(name=self.queueName).add(task)
        key = db.Key.from_path(_FantasmInstance.kind(), self.instanceName, namespace='')
        _FantasmInstance(key=key, instanceName=self.instanceName).put()

        return FSM.PSEUDO_INIT

    def dispatch(self, event, obj):
        """ The main entry point to move the machine according to an event.

        @param event: a string event to dispatch to the FSMContext
        @param obj: an object that the FSMContext can operate on
        @return: an event string to dispatch to the FSMContext
        """

        self.__obj = self.__obj or obj # hold the obj object for use during this context

        # store the starting state and event for the handleEvent() method
        self.startingState = self.currentState
        self.startingEvent = event

        nextEvent = None
        try:
            nextEvent = self.currentState.dispatch(self, event, obj)

            if obj.get(constants.FORKED_CONTEXTS_PARAM):
                # pylint: disable=W0212
                # - accessing the protected method is fine here, since it is an instance of the same class
                tasks = []
                for context in obj[constants.FORKED_CONTEXTS_PARAM]:
                    context[constants.STEPS_PARAM] = int(context.get(constants.STEPS_PARAM, '0')) + 1
                    task = context.queueDispatch(nextEvent, queue=False)
                    if task: # fan-in magic
                        if not task.was_enqueued: # fan-in always queues
                            tasks.append(task)

                try:
                    if tasks:
                        transition = self.currentState.getTransition(nextEvent)
                        _queueTasks(self.Queue, transition.queueName, tasks)

                except (TaskAlreadyExistsError, TombstonedTaskError):
                    # unlike a similar block in self.continutation, this is well off the happy path
                    self.logger.critical(
                                     'Unable to queue fork Tasks %s as it/they already exists. (Machine %s, State %s)',
                                     [task.name for task in tasks if not task.was_enqueued],
                                     self.machineName,
                                     self.currentState.name)

            if nextEvent:
                self[constants.STEPS_PARAM] = int(self.get(constants.STEPS_PARAM, '0')) + 1

                try:
                    self.queueDispatch(nextEvent)

                except (TaskAlreadyExistsError, TombstonedTaskError):
                    # unlike a similar block in self.continutation, this is well off the happy path
                    #
                    # FIXME: when this happens, it means there was failure shortly after queuing the Task, or
                    #        possibly even with queuing the Task. when this happens there is a chance that
                    #        two states in the machine are executing simultaneously, which is may or may not
                    #        be a good thing, depending on what each state does. gracefully handling this
                    #        exception at least means that this state will terminate.
                    # NOTE:  This happens most often due to oddities in the Task Queue system. E.g., task
                    #        queue will raise an exception while doing a BulkAdd or a TransientError occurs,
                    #        however despite this error, the task was enqueued (and the task name was
                    #        registered in the tombstone system). That is, it seems that most often, the
                    #        error that leads to this message does not mean that a particular machine did
                    #        not continue; it probably always does continue. However, not that the particular
                    #        state that issues this particular warning was triggered twice, so any side-effect
                    #        (e.g., sending an email) would have occurred twice. Remember it is very important
                    #        for your machine states to be idemopotent meaning they have to protect against
                    #        this situation on their own as the task queue system itself is distributed and
                    #        definitely not perfect.
                    self.logger.warning('Unable to queue next Task as it already exists. (Machine %s, State %s)',
                                     self.machineName,
                                     self.currentState.name)

            else:
                # if we're not in a final state, emit a log message
                # FIXME - somehow we should avoid this message if we're in the "last" step of a continuation...
                if not self.currentState.isFinalState and not obj.get(constants.TERMINATED_PARAM):
                    self.logger.critical('Non-final state did not emit an event. Machine has terminated in an ' +
                                     'unknown state. (Machine %s, State %s)' %
                                     (self.machineName, self.currentState.name))
                # if it is a final state, then dispatch the pseudo-final event to finalize the state machine
                elif self.currentState.isFinalState and self.currentState.exitAction:
                    self[constants.STEPS_PARAM] = int(self.get(constants.STEPS_PARAM, '0')) + 1
                    self.queueDispatch(FSM.PSEUDO_FINAL)

        except HaltMachineError as e:
            if e.level is not None and e.message:
                self.logger.log(e.level, e.message)
            return None # stop the machine
        except Exception as e:
            level = self.logger.error
            if e.__class__ in TRANSIENT_ERRORS:
                level = self.logger.warn
            level("FSMContext.dispatch is handling the following exception:", exc_info=True)
            self._handleException(event, obj)

        return nextEvent

    def continuation(self, nextToken):
        """ Performs a continuation be re-queueing an FSMContext Task with a slightly modified continuation
        token. self.startingState and self.startingEvent are used in the re-queue, so this can be seen as a
        'fork' of the current context.

        @param nextToken: the next continuation token
        """
        assert not self.get(constants.INDEX_PARAM) # fan-out after fan-in is not allowed
        step = str(self[constants.STEPS_PARAM]) # needs to be a str key into a json dict

        # make a copy and set the currentState to the startingState of this context
        context = self.clone()
        context.currentState = self.startingState

        # update the generation and continuation params
        gen = context.get(constants.GEN_PARAM, {})
        gen[step] = gen.get(step, 0) + 1
        context[constants.GEN_PARAM] = gen
        context[constants.CONTINUATION_PARAM] = nextToken

        try:
            # pylint: disable=W0212
            # - accessing the protected method is fine here, since it is an instance of the same class
            transition = self.startingState.getTransition(self.startingEvent)
            countdown = self.currentState.continuationCountdown
            context._queueDispatchNormal(self.startingEvent, queue=True, queueName=transition.queueName,
                                         retryOptions=transition.retryOptions, taskTarget=transition.taskTarget,
                                         countdown=countdown)

        except (TaskAlreadyExistsError, TombstonedTaskError):
            # this can happen when currentState.dispatch() previously succeeded in queueing the continuation
            # Task, but failed with the doAction.execute() call in a _previous_ execution of this Task.
            # NOTE: this prevent the dreaded "fork bomb"
            self.logger.info('Unable to queue continuation Task as it already exists. (Machine %s, State %s)',
                          self.machineName,
                          self.currentState.name)

    def setQueue(self, queueName):
        """ Used to override the queue defined in fsm.yaml, e.g., for dynamic queue selection. """
        if self.headers is None:
            self.headers = {}
        self.headers[constants.HTTP_REQUEST_HEADER_QUEUENAME] = queueName

    def queueDispatch(self, nextEvent, queue=True):
        """ Queues a .dispatch(nextEvent) call in the appengine Task queue.

        @param nextEvent: a string event
        @param queue: a boolean indicating whether or not to queue a Task, or leave it to the caller
        @return: a taskqueue.Task instance which may or may not have been queued already
        """
        assert nextEvent is not None

        # self.currentState is already transitioned away from self.startingState
        transition = self.currentState.getTransition(nextEvent)
        queueName = transition.queueName
        if self.headers and self.headers.get(constants.HTTP_REQUEST_HEADER_QUEUENAME):
            queueName = self.headers[constants.HTTP_REQUEST_HEADER_QUEUENAME]
        if transition.target.isFanIn:
            task = self._queueDispatchFanIn(nextEvent, fanInPeriod=transition.target.fanInPeriod,
                                            retryOptions=transition.retryOptions,
                                            queueName=queueName, taskTarget=transition.taskTarget)
        else:
            countdown = transition.countdown
            if isinstance(countdown, tuple): # (minumum, maximum), randomly choose
                countdown = random.randint(countdown[0], countdown[1])
            task = self._queueDispatchNormal(nextEvent, queue=queue, countdown=countdown,
                                             retryOptions=transition.retryOptions,
                                             queueName=queueName, taskTarget=transition.taskTarget)

        return task

    def _queueDispatchNormal(self, nextEvent, queue=True, countdown=0, retryOptions=None, queueName=None,
                             taskTarget=None):
        """ Queues a call to .dispatch(nextEvent) in the appengine Task queue.

        @param nextEvent: a string event
        @param queue: a boolean indicating whether or not to queue a Task, or leave it to the caller
        @param countdown: the number of seconds to countdown before the queued task fires
        @param retryOptions: the RetryOptions for the task
        @param queueName: the queue name to Queue into
        @param taskTarget: the task target parameter
        @return: a taskqueue.Task instance which may or may not have been queued already
        """
        assert nextEvent is not None
        assert queueName

        url = self.buildUrl(self.currentState, nextEvent)
        params = self.buildParams(self.currentState, nextEvent)
        taskName = self.getTaskName(nextEvent)

        task = Task(name=taskName, method=self.method, url=url, params=params, countdown=countdown,
                    retry_options=retryOptions, headers=self.headers, target=taskTarget)
        if queue:
            self.Queue(name=queueName).add(task)
            if not task.was_enqueued:
                self.logger.critical('Task "%s" was not enqueued.', taskName)

        return task

    def _queueDispatchFanIn(self, nextEvent, fanInPeriod=0, retryOptions=None, queueName=None, taskTarget=None):
        """ Queues a call to .dispatch(nextEvent) in the task queue, or saves the context to the
        datastore for processing by the queued .dispatch(nextEvent)

        @param nextEvent: a string event
        @param fanInPeriod: the period of time between fan in Tasks
        @param queueName: the queue name to Queue into
        @param taskTarget: the task target parameter
        @return: a taskqueue.Task instance which may or may not have been queued already
        """
        assert nextEvent is not None
        assert not self.get(constants.INDEX_PARAM) # fan-in after fan-in is not allowed
        assert queueName

        # we pop this off here because we do not want the fan-out/continuation param as part of the
        # task name, otherwise we loose the fan-in - each fan-in gets one work unit.
        self.pop(constants.GEN_PARAM, None)
        fork = self.pop(constants.FORK_PARAM, None)

        # transfer the fan-in-group into the context (under a fixed value key) so that states beyond
        # the fan-in get unique Task names
        # FIXME: this will likely change once we formalize what to do post fan-in
        transition = self.currentState.getTransition(nextEvent)
        if self.get(transition.target.fanInGroup) is not None:
            self[constants.FAN_IN_GROUP_PARAM] = self[transition.target.fanInGroup]

        taskNameBase = self.getTaskName(nextEvent, fanIn=True)
        rwlock = ReadWriteLock(taskNameBase, self)
        index = rwlock.currentIndex()

        # (***)
        #
        # grab the lock - memcache.incr()
        #
        # on Task retry, multiple incr() calls are possible. possible ways to handle:
        #
        # 1. release the lock in a 'finally' clause, but then risk missing a work
        #    package because acquiring the read lock will succeed even though the
        #    work package was not written yet.
        #
        # 2. allow the lock to get too high. the fan-in logic attempts to wait for
        #    work packages across multiple-retry attempts, so this seems like the
        #    best option. we basically trade a bit of latency in fan-in for reliability.
        #
        rwlock.acquireWriteLock(index, nextEvent=nextEvent)

        # insert the work package, which is simply a serialized FSMContext
        workIndex = '%s-%d' % (taskNameBase, knuthHash(index))

        # on retry, we want to ensure we get the same work index for this task
        actualTaskName = self.__obj[constants.TASK_NAME_PARAM]
        indexKeyName = 'workIndex-' + '-'.join([str(i) for i in [actualTaskName, fork] if i]) or None
        semaphore = RunOnceSemaphore(indexKeyName, self)

        # check if the workIndex changed during retry
        semaphoreWritten = False
        if self.__obj[constants.RETRY_COUNT_PARAM] > 0:
            # see comment (A) in self._queueDispatchFanIn(...)
            time.sleep(constants.DATASTORE_ASYNCRONOUS_INDEX_WRITE_WAIT_TIME)
            payload = semaphore.readRunOnceSemaphore(payload=workIndex, transactional=False)
            if payload:
                semaphoreWritten = True
                if payload != workIndex:
                    self.logger.info("Work index changed from '%s' to '%s' on retry.", payload, workIndex)
                    workIndex = payload

        # update this here so it gets written down into the work package too
        self[constants.INDEX_PARAM] = index

        # write down two models, one actual work package, one idempotency package
        keyName = '-'.join([str(i) for i in [actualTaskName, fork] if i]) or None
        key = db.Key.from_path(_FantasmFanIn.kind(), keyName, namespace='')
        work = _FantasmFanIn(context=self, workIndex=workIndex, key=key)

        # close enough to idempotent, but could still write only one of the entities
        # FIXME: could be made faster using a bulk put, but this interface is cleaner
        if not semaphoreWritten:
            semaphore.writeRunOnceSemaphore(payload=workIndex, transactional=False)

        # put the work item
        db.put(work)

        # (A) now the datastore is asynchronously writing the indices, so the work package may
        #     not show up in a query for a period of time. there is a corresponding time.sleep()
        #     in the fan-in of self.mergeJoinDispatch(...)

        # release the lock - memcache.decr()
        rwlock.releaseWriteLock(index)

        try:

            # insert a task to run in the future and process a bunch of work packages
            now = time.time()
            url = self.buildUrl(self.currentState, nextEvent)
            params = self.buildParams(self.currentState, nextEvent)
            taskName = '%s-%d' % (taskNameBase, index)
            task = Task(name=taskName,
                        method=self.method,
                        url=url,
                        params=params,
                        eta=datetime.datetime.utcfromtimestamp(now) + datetime.timedelta(seconds=fanInPeriod),
                        headers=self.headers,
                        retry_options=retryOptions,
                        target=taskTarget)
            self.Queue(name=queueName).add(task)
            if not task.was_enqueued:
                self.logger.critical('Task "%s" was not enqueued.', taskName)
            return task

        except (TaskAlreadyExistsError, TombstonedTaskError):
            pass # Fan-in magic


    def mergeJoinDispatch(self, event, obj):
        """ Performs a merge join on the pending fan-in dispatches.

        @param event: an event that is being merge joined (destination state must be a fan in)
        @return: a list (possibly empty) of FSMContext instances
        """
        # this assertion comes from _queueDispatchFanIn - we never want fan-out info in a fan-in context
        assert not self.get(constants.GEN_PARAM)
        assert not self.get(constants.FORK_PARAM)

        # the work package index is stored in the url of the Task/FSMContext
        index = self.get(constants.INDEX_PARAM)
        self.logger.debug('Index: %s', index)
        taskNameBase = self.getTaskName(event, fanIn=True)

        # see comment (***) in self._queueDispatchFanIn
        #
        # in the case of failing to acquire a read lock (due to failed release of write lock)
        # we have decided to keep retrying
        raiseOnFail = False
        if self._getTaskRetryLimit() is not None:
            raiseOnFail = (self._getTaskRetryLimit() > self.__obj[constants.RETRY_COUNT_PARAM])

        rwlock = ReadWriteLock(taskNameBase, self)
        rwlock.acquireReadLock(index, raiseOnFail=raiseOnFail)

        # and return the FSMContexts list
        class FSMContextList(list):
            """ A list that supports .logger.info(), .logger.warning() etc.for fan-in actions """
            def __init__(self, context, contexts, guarded=False):
                """ setup a self.logger for fan-in actions """
                super().__init__(contexts)
                self.logger = Logger(context)
                self.instanceName = context.instanceName
                self.guarded = guarded

        # see comment (A) in self._queueDispatchFanIn(...)
        time.sleep(constants.DATASTORE_ASYNCRONOUS_INDEX_WRITE_WAIT_TIME)

        # the following step ensure that fan-in only ever operates one time over a list of data
        # the entity is created in State.dispatch(...) _after_ all the actions have executed
        # successfully
        khash = knuthHash(index)
        self.logger.debug('knuthHash of index: %s', khash)
        workIndex = '%s-%d' % (taskNameBase, khash)
        if obj[constants.RETRY_COUNT_PARAM] > 0:
            semaphore = RunOnceSemaphore(workIndex, self)
            if semaphore.readRunOnceSemaphore(payload=self.__obj[constants.TASK_NAME_PARAM]):
                self.logger.info("Fan-in idempotency guard for workIndex '%s', not processing any work items.",
                                 workIndex)
                return FSMContextList(self, [], guarded=True) # don't operate over the data again

        # fetch all the work packages in the current group for processing
        query = _FantasmFanIn.all(namespace='') \
                             .filter('workIndex =', workIndex) \
                             .order('__key__')

        # construct a list of FSMContexts
        contexts = [self.clone(replaceData=r.context) for r in query]
        return FSMContextList(self, contexts)

    def _getTaskRetryLimit(self):
        """ Method that returns the maximum number of retries for this particular dispatch

        @param obj: an object that the FSMContext can operate on
        """
        # get task_retry_limit configuration
        try:
            transition = self.startingState.getTransition(self.startingEvent)
            taskRetryLimit = transition.retryOptions.task_retry_limit
        except UnknownEventError:
            # can't find the transition, use the machine-level default
            taskRetryLimit = self.retryOptions.task_retry_limit
        return taskRetryLimit

    def _handleException(self, event, obj):
        """ Method for child classes to override to handle exceptions.

        @param event: a string event
        @param obj: an object that the FSMContext can operate on
        """
        retryCount = obj.get(constants.RETRY_COUNT_PARAM, 0)
        taskRetryLimit = self._getTaskRetryLimit()

        if taskRetryLimit and retryCount >= taskRetryLimit:
            # need to permanently fail
            self.logger.critical('Max-requeues reached. Machine has terminated in an unknown state. ' +
                             '(Machine %s, State %s, Event %s)',
                             self.machineName, self.startingState.name, event, exc_info=True)
            # re-raise, letting App Engine TaskRetryOptions kill the task
            raise
        else:
            # re-raise the exception
            self.logger.warning('Exception occurred processing event. Task will be retried. ' +
                            '(Machine %s, State %s)',
                            self.machineName, self.startingState.name, exc_info=True)

            # this line really just allows unit tests to work - the request is really dead at this point
            self.currentState = self.startingState

            raise

    def buildUrl(self, state, event):
        """ Builds the taskqueue url.

        @param state: the State to dispatch to
        @param event: the event to dispatch
        @return: a url that can be used to build a taskqueue.Task instance to .dispatch(event)
        """
        assert state and event
        return self.url + '{}/{}/{}/'.format(state.name,
                                         event,
                                         state.getTransition(event).target.name)

    def buildParams(self, state, event):
        """ Builds the taskqueue params.

        @param state: the State to dispatch to
        @param event: the event to dispatch
        @return: a dict suitable to use in constructing a url (GET) or using as params (POST)
        """
        assert state and event
        params = {constants.STATE_PARAM: state.name,
                  constants.EVENT_PARAM: event,
                  constants.INSTANCE_NAME_PARAM: self.instanceName}
        for key, value in list(self.items()):
            if key not in constants.NON_CONTEXT_PARAMS:
                if self.contextTypes.get(key) is json.loads:
                    value = json.dumps(value, cls=models.Encoder)
                if self.contextTypes.get(key) is pickle.loads:
                    value = base64.urlsafe_b64encode(pickle.dumps(value))
                if self.contextTypes.get(key) is config.deserializeNDBKey:
                    value = value.urlsafe()
                if isinstance(value, dict):
                    # FIXME: should we issue a warning that they should update fsm.yaml?
                    value = json.dumps(value, cls=models.Encoder)

                valueIsNotBasestring = False
                if isinstance(value, (list, tuple)):
                    for v in value:
                        if not isinstance(v, (str, bytes)):
                            valueIsNotBasestring = True
                elif not isinstance(value, (str, bytes)):
                    valueIsNotBasestring = True

                if valueIsNotBasestring:
                    if key not in list(self.contextTypes.keys()):
                        self.logger.warning("Attempting to put an object in the FSMContext without specifying an "
                                            "entry for key '%s' in 'context_types' in the yaml for machineName '%s'. "
                                            "There will likely be conversion issues (ie. booleans turned into "
                                            "strings).", key, self.machineName)

                if isinstance(value, (list, tuple)) and len(value) == 1:
                    key = key + '[]' # used to preserve lists of length=1 - see handler.py for inverse

                params[key] = value
        return params

    def getTaskName(self, nextEvent, instanceName=None, fanIn=False):
        """ Returns a task name that is unique for a specific dispatch

        @param nextEvent: the event to dispatch
        @return: a task name that can be used to build a taskqueue.Task instance to .dispatch(nextEvent)
        """
        transition = self.currentState.getTransition(nextEvent)
        parts = []
        parts.append(instanceName or self.instanceName)

        if self.get(constants.GEN_PARAM):
            for (step, gen) in list(self[constants.GEN_PARAM].items()):
                parts.append('continuation-{}-{}'.format(step, gen))
        if self.get(constants.FORK_PARAM):
            parts.append('fork-' + str(self[constants.FORK_PARAM]))
        # post-fan-in we need to store the workIndex in the task name to avoid duplicates, since
        # we popped the generation off during fan-in
        # FIXME: maybe not pop the generation in fan-in?
        # FIXME: maybe store this in the instanceName?
        # FIXME: i wish this was easier to get right :-)
        if (not fanIn) and self.get(constants.INDEX_PARAM):
            parts.append('work-index-' + str(self[constants.INDEX_PARAM]))
        parts.append(self.currentState.name)
        parts.append(nextEvent)
        parts.append(transition.target.name)
        parts.append('step-' + str(self[constants.STEPS_PARAM]))
        if self.get(constants.FAN_IN_GROUP_PARAM) is not None:
            parts.append('group-' + str(self[constants.FAN_IN_GROUP_PARAM]))
        return '--'.join(parts)

    def clone(self, instanceName=None, updateData=None, replaceData=None):
        """ Returns a copy of the FSMContext.

        @param instanceName: the instance name to optionally apply to the clone
        @param updateData: a dict/mapping of data to optionally apply (.update()) to the clone
        @param replaceData: a dict/mapping of data to optionally apply (.clear()/.update()) to the clone
        @return: a new FSMContext instance
        """
        assert (not updateData) or (not replaceData), "cannot update and replace data at the same time"

        #context = copy.deepcopy(self)
        # shallow copy the context
        context = copy.copy(self)

        # deepcopy the dictionary portion of the context
        deepcopy_dict = copy.deepcopy(dict(self))
        context.update(deepcopy_dict)

        if instanceName:
            context.instanceName = instanceName
        if updateData:
            context.update(updateData)
        if replaceData:
            context.clear()
            context.update(replaceData)
        return context

# pylint: disable=C0103
def _queueTasks(Queue, queueName, tasks, transactional=False):
    """
    Add a list of Tasks to the supplied Queue/queueName

    @param Queue: taskqueue.Queue or other object with .add() method
    @param queueName: a queue name from queue.yaml
    @param tasks: a list of taskqueue.Tasks
    @param transactional: tasks are only queued if transaction commits successfully; default False

    @raise TaskAlreadyExistsError:
    @raise TombstonedTaskError:
    """

    from google.appengine.api.taskqueue.taskqueue import MAX_TASKS_PER_ADD
    taskAlreadyExists, tombstonedTask = None, None

    # queue the Tasks in groups of MAX_TASKS_PER_ADD
    i = 0
    for i in range(len(tasks)):
        someTasks = tasks[i * MAX_TASKS_PER_ADD : (i+1) * MAX_TASKS_PER_ADD]
        if not someTasks:
            break

        # queue them up, and loop back for more, even if there are failures
        try:
            Queue(name=queueName).add(someTasks, transactional=transactional)

        except TaskAlreadyExistsError as e:
            taskAlreadyExists = e

        except TombstonedTaskError as e:
            tombstonedTask = e

    if taskAlreadyExists:
        # pylint: disable=E0702
        raise taskAlreadyExists

    if tombstonedTask:
        # pylint: disable=E0702
        raise tombstonedTask

def startStateMachine(machineName, contexts, taskName=None, method='POST', countdown=0,
                      _currentConfig=None, headers=None, raiseIfTaskExists=False, transactional=False,
                      queueName=None):
    """ Starts a new machine(s), by simply queuing a task.

    @param machineName the name of the machine in the FSM to start
    @param contexts a list of contexts to start the machine with; a machine will be started for each context
    @param taskName used for idempotency; will become the root of the task name for the actual task queued
    @param method the HTTP methld (GET/POST) to run the machine with (default 'POST')
    @param countdown the number of seconds into the future to start the machine (default 0 - immediately)
                     or a list of sumber of seconds (must be same length as contexts)
    @param headers: a dict of X-Fantasm request headers to pass along in Tasks
    @param raiseIfTaskExists: a bool indicating method should re-raise TaskAlreadyExistsError and TombstonedTaskErrors
    @param transactional: task to start machine is only emitted if the transaction succeeds (default: False)
    @param queueName: The queue to use for the machine. Note this queue is only used _after_ the initialization task,
                      which will still be queued up on the machine default. This allows a single switch to halt
                      new machines, but still allows for dynamically running machines on non-default queues.

    @param _currentConfig used for test injection (default None - use fsm.yaml definitions)
    """
    if not contexts:
        return
    if not isinstance(contexts, list):
        contexts = [contexts]
    if not isinstance(countdown, list):
        countdown = [countdown] * len(contexts)

    # FIXME: I shouldn't have to do this.
    for context in contexts:
        context[constants.STEPS_PARAM] = 0

    fsm = FSM(currentConfig=_currentConfig) # loads the FSM definition

    if queueName:
        if not headers:
            headers = {}
        if constants.HTTP_REQUEST_HEADER_QUEUENAME in headers:
            logging.warn('queueName "%s" overrides existing queueName in headers "%s". Using former.' % \
                         queueName, headers[constants.HTTP_REQUEST_HEADER_QUEUENAME])
        headers[constants.HTTP_REQUEST_HEADER_QUEUENAME] = queueName

    instances = [fsm.createFSMInstance(machineName, data=context, method=method, headers=headers)
                 for context in contexts]

    tasks = []
    for i, instance in enumerate(instances):
        tname = None
        if taskName:
            tname = '%s--startStateMachine-%d' % (taskName, i)
        task = instance.generateInitializationTask(countdown=countdown[i], taskName=tname, transactional=transactional)
        tasks.append(task)

    initialQueueName = instances[0].queueName # same machineName, same queues
    try:
        from google.appengine.api.taskqueue.taskqueue import Queue
        _queueTasks(Queue, initialQueueName, tasks, transactional=transactional)
    except (TaskAlreadyExistsError, TombstonedTaskError):
        # FIXME: what happens if _some_ of the tasks were previously enqueued?
        # normal result for idempotency
        import logging
        logging.info('Unable to queue new machine %s with taskName %s as it has been previously enqueued.',
                      machineName, taskName)
        if raiseIfTaskExists:
            raise
