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
"""
import logging

from google.appengine.api import urlfetch_errors, datastore_errors, taskqueue
from google.appengine.runtime import apiproxy_errors

from fantasm import constants

class UserTransientError(Exception):
    """ Can be raised by client code so that only warning-level messages are emitted. """
    pass

# The following exceptions are "transient" errors that can occur on Google App Engine.
# Since one of Fantasm's primary advantages is to retry when these occur, this list of
# errors will only be logged at "warn" level (instead of "error" level). If you have
# other transient errors you want to raise for a retry, but to be logged at the lower
# "warn" level, then raise a UserTransientError.
TRANSIENT_ERRORS = [
    urlfetch_errors.DownloadError,
    urlfetch_errors.InternalTransientError,
    urlfetch_errors.ConnectionClosedError,
    urlfetch_errors.DeadlineExceededError,
    datastore_errors.TransactionFailedError,
    datastore_errors.InternalError,
    datastore_errors.Timeout,
    apiproxy_errors.DeadlineExceededError,
    taskqueue.TransientError,
    taskqueue.InternalError,
    UserTransientError,
]

class HaltMachineError(Exception):
    """ Raise this exception in your code to cause the machine to halt. """
    def __init__(self, message, logLevel=logging.WARNING):
        self.message = message
        self.level = logLevel
        super().__init__(message)

class FSMRuntimeError(Exception):
    """ The parent class of all Fantasm runtime errors. """
    pass

class UnknownMachineError(FSMRuntimeError):
    """ A machine could not be found. """
    def __init__(self, machineName):
        """ Initialize exception """
        message = 'Cannot find machine "%s".' % machineName
        super().__init__(message)

class UnknownStateError(FSMRuntimeError):
    """ A state could not be found  """
    def __init__(self, machineName, stateName):
        """ Initialize exception """
        message = 'State "{}" is unknown. (Machine {})'.format(stateName, machineName)
        super().__init__(message)

class UnknownEventError(FSMRuntimeError):
    """ An event and the transition bound to it could not be found. """
    def __init__(self, event, machineName, stateName):
        """ Initialize exception """
        message = 'Cannot find transition for event "{}". (Machine {}, State {})'.format(event, machineName, stateName)
        super().__init__(message)

class InvalidEventNameRuntimeError(FSMRuntimeError):
    """ Event returned from dispatch is invalid (and would cause problems with task name restrictions). """
    def __init__(self, event, machineName, stateName, instanceName):
        """ Initialize exception """
        message = 'Event "%r" returned by state is invalid. It must be a string and match pattern "%s". ' \
                  '(Machine %s, State %s, Instance %s)' % \
                  (event, constants.NAME_PATTERN, machineName, stateName, instanceName)
        super().__init__(message)

class InvalidFinalEventRuntimeError(FSMRuntimeError):
    """ Event returned when a final state action returns an event. """
    def __init__(self, event, machineName, stateName, instanceName):
        """ Initialize exception """
        message = 'Event "%r" returned by final state is invalid. ' \
                  '(Machine %s, State %s, Instance %s)' % \
                  (event, machineName, stateName, instanceName)
        super().__init__(message)

class FanInWriteLockFailureRuntimeError(FSMRuntimeError):
    """ Exception when fan-in writers are unable to acquire a lock. """
    def __init__(self, event, machineName, stateName, instanceName):
        """ Initialize exception """
        message = 'Event "%r" unable to to be fanned-in due to write lock failure. ' \
                  '(Machine %s, State %s, Instance %s)' % \
                  (event, machineName, stateName, instanceName)
        super().__init__(message)

class FanInReadLockFailureRuntimeError(FSMRuntimeError):
    """ Exception when fan-in readers are unable to acquire a lock. """
    def __init__(self, event, machineName, stateName, instanceName):
        """ Initialize exception """
        message = 'Event "%r" unable to to be fanned-in due to read lock failure. ' \
                  '(Machine %s, State %s, Instance %s)' % \
                  (event, machineName, stateName, instanceName)
        super().__init__(message)

class FanInNoContextsAvailableRuntimeError(FSMRuntimeError):
    """ Exception when fan-in results in 0 contexts - ie. appengine index write timing issue. """
    def __init__(self, event, machineName, stateName, instanceName):
        """ Initialize exception """
        message = 'Fan-in resulted in 0 contexts. (Event %s, Machine %s, State %s, Instance %s)' % \
                  (event, machineName, stateName, instanceName)
        super().__init__(message)

class RequiredServicesUnavailableRuntimeError(FSMRuntimeError):
    """ Some of the required API services are not available. """
    def __init__(self, unavailableServices):
        """ Initialize exception """
        message = 'The following services will not be available in the %d seconds: %s. This task will be retried.' % \
                  (constants.REQUEST_LENGTH, unavailableServices)
        super().__init__(message)

class ConfigurationError(Exception):
    """ Parent class for all Fantasm configuration errors. """
    pass

class YamlFileNotFoundError(ConfigurationError):
    """ The Yaml file could not be found. """
    def __init__(self, filename):
        """ Initialize exception """
        message = 'Yaml configuration file "%s" not found.' % filename
        super().__init__(message)

class YamlFileCircularImportError(ConfigurationError):
    """ The Yaml is involved in a circular import. """
    def __init__(self, filename):
        """ Initialize exception """
        message = 'Yaml configuration file "%s" involved in a circular import.' % filename
        super().__init__(message)

class StateMachinesAttributeRequiredError(ConfigurationError):
    """ The YAML file requires a 'state_machines' attribute. """
    def __init__(self):
        """ Initialize exception """
        message = '"%s" is required attribute of yaml file.' % constants.STATE_MACHINES_ATTRIBUTE
        super().__init__(message)

class MachineNameRequiredError(ConfigurationError):
    """ Each machine requires a name. """
    def __init__(self):
        """ Initialize exception """
        message = '"%s" is required attribute of machine.' % constants.MACHINE_NAME_ATTRIBUTE
        super().__init__(message)

class InvalidQueueNameError(ConfigurationError):
    """ The queue name was not valid. """
    def __init__(self, queueName, machineName):
        """ Initialize exception """
        message = 'Queue name "{}" must exist in queue.yaml. (Machine {})'.format(queueName, machineName)
        super().__init__(message)

class InvalidMachineNameError(ConfigurationError):
    """ The machine name was not valid. """
    def __init__(self, machineName):
        """ Initialize exception """
        message = 'Machine name must match pattern "{}". (Machine {})'.format(constants.NAME_PATTERN, machineName)
        super().__init__(message)

class MachineNameNotUniqueError(ConfigurationError):
    """ Each machine in a YAML file must have a unique name. """
    def __init__(self, machineName):
        """ Initialize exception """
        message = 'Machine names must be unique. (Machine %s)' % machineName
        super().__init__(message)

class MachineHasMultipleInitialStatesError(ConfigurationError):
    """ Each machine must have exactly one initial state. """
    def __init__(self, machineName):
        """ Initialize exception """
        message = 'Machine has multiple initial states, but only one is allowed. (Machine %s)' % machineName
        super().__init__(message)

class MachineHasNoInitialStateError(ConfigurationError):
    """ Each machine must have exactly one initial state. """
    def __init__(self, machineName):
        """ Initialize exception """
        message = 'Machine has no initial state, exactly one is required. (Machine %s)' % machineName
        super().__init__(message)

class MachineHasNoFinalStateError(ConfigurationError):
    """ Each machine must have at least one final state. """
    def __init__(self, machineName):
        """ Initialize exception """
        message = 'Machine has no final states, but at least one is required. (Machine %s)' % machineName
        super().__init__(message)

class StateNameRequiredError(ConfigurationError):
    """ Each state requires a name. """
    def __init__(self, machineName):
        """ Initialize exception """
        message = '"{}" is required attribute of state. (Machine {})'.format(constants.STATE_NAME_ATTRIBUTE, machineName)
        super().__init__(message)

class InvalidStateNameError(ConfigurationError):
    """ The state name was not valid. """
    def __init__(self, machineName, stateName):
        """ Initialize exception """
        message = 'State name must match pattern "%s". (Machine %s, State %s)' % \
                  (constants.NAME_PATTERN, machineName, stateName)
        super().__init__(message)

class StateNameNotUniqueError(ConfigurationError):
    """ Each state within a machine must have a unique name. """
    def __init__(self, machineName, stateName):
        """ Initialize exception """
        message = 'State names within a machine must be unique. (Machine %s, State %s)' % \
                  (machineName, stateName)
        super().__init__(message)

class StateActionRequired(ConfigurationError):
    """ Each state requires an action. """
    def __init__(self, machineName, stateName):
        """ Initialize exception """
        message = '"%s" is required attribute of state. (Machine %s, State %s)' % \
                  (constants.STATE_ACTION_ATTRIBUTE, machineName, stateName)
        super().__init__(message)

class UnknownModuleError(ConfigurationError):
    """ When resolving actions, the module was not found. """
    def __init__(self, moduleName, importError):
        """ Initialize exception """
        message = 'Module "{}" cannot be imported due to "{}".'.format(moduleName, importError)
        super().__init__(message)

class UnknownClassError(ConfigurationError):
    """ When resolving actions, the class was not found. """
    def __init__(self, moduleName, className):
        """ Initialize exception """
        message = 'Class "{}" was not found in module "{}".'.format(className, moduleName)
        super().__init__(message)

class UnknownObjectError(ConfigurationError):
    """ When resolving actions, the object was not found. """
    def __init__(self, objectName):
        """ Initialize exception """
        message = 'Object "%s" was not found.' % (objectName)
        super().__init__(message)

class UnexpectedObjectTypeError(ConfigurationError):
    """ When resolving actions, the object was not found. """
    def __init__(self, objectName, expectedType):
        """ Initialize exception """
        message = 'Object "{}" is not of type "{}".'.format(objectName, expectedType)
        super().__init__(message)

class InvalidMaxRetriesError(ConfigurationError):
    """ max_retries must be a positive integer. """
    def __init__(self, machineName, maxRetries):
        """ Initialize exception """
        message = '%s "%s" is invalid. Must be an integer. (Machine %s)' % \
                  (constants.MAX_RETRIES_ATTRIBUTE, maxRetries, machineName)
        super().__init__(message)

class InvalidTaskRetryLimitError(ConfigurationError):
    """ task_retry_limit must be a positive integer. """
    def __init__(self, machineName, taskRetryLimit):
        """ Initialize exception """
        message = '%s "%s" is invalid. Must be an integer. (Machine %s)' % \
                  (constants.TASK_RETRY_LIMIT_ATTRIBUTE, taskRetryLimit, machineName)
        super().__init__(message)

class InvalidMinBackoffSecondsError(ConfigurationError):
    """ min_backoff_seconds must be a positive integer. """
    def __init__(self, machineName, minBackoffSeconds):
        """ Initialize exception """
        message = '%s "%s" is invalid. Must be an integer. (Machine %s)' % \
                  (constants.MIN_BACKOFF_SECONDS_ATTRIBUTE, minBackoffSeconds, machineName)
        super().__init__(message)

class InvalidMaxBackoffSecondsError(ConfigurationError):
    """ max_backoff_seconds must be a positive integer. """
    def __init__(self, machineName, maxBackoffSeconds):
        """ Initialize exception """
        message = '%s "%s" is invalid. Must be an integer. (Machine %s)' % \
                  (constants.MAX_BACKOFF_SECONDS_ATTRIBUTE, maxBackoffSeconds, machineName)
        super().__init__(message)

class InvalidTaskAgeLimitError(ConfigurationError):
    """ task_age_limit must be a positive integer. """
    def __init__(self, machineName, taskAgeLimit):
        """ Initialize exception """
        message = '%s "%s" is invalid. Must be an integer. (Machine %s)' % \
                  (constants.TASK_AGE_LIMIT_ATTRIBUTE, taskAgeLimit, machineName)
        super().__init__(message)

class InvalidMaxDoublingsError(ConfigurationError):
    """ max_doublings must be a positive integer. """
    def __init__(self, machineName, maxDoublings):
        """ Initialize exception """
        message = '%s "%s" is invalid. Must be an integer. (Machine %s)' % \
                  (constants.MAX_DOUBLINGS_ATTRIBUTE, maxDoublings, machineName)
        super().__init__(message)

class MaxRetriesAndTaskRetryLimitMutuallyExclusiveError(ConfigurationError):
    """ max_retries and task_retry_limit cannot both be specified on a machine. """
    def __init__(self, machineName):
        """ Initialize exception """
        message = 'max_retries and task_retry_limit cannot both be specified on a machine. (Machine %s)' % \
                  machineName
        super().__init__(message)

class InvalidLoggingError(ConfigurationError):
    """ The logging value was not valid. """
    def __init__(self, machineName, loggingValue):
        """ Initialize exception """
        message = 'logging attribute "%s" is invalid (must be one of "%s"). (Machine %s)' % \
                  (loggingValue, constants.VALID_LOGGING_VALUES, machineName)
        super().__init__(message)

class TransitionNameRequiredError(ConfigurationError):
    """ Each transition requires a name. """
    def __init__(self, machineName):
        """ Initialize exception """
        message = '"%s" is required attribute of transition. (Machine %s)' % \
                  (constants.TRANS_NAME_ATTRIBUTE, machineName)
        super().__init__(message)

class InvalidTransitionNameError(ConfigurationError):
    """ The transition name was invalid. """
    def __init__(self, machineName, transitionName):
        """ Initialize exception """
        message = 'Transition name must match pattern "%s". (Machine %s, Transition %s)' % \
                  (constants.NAME_PATTERN, machineName, transitionName)
        super().__init__(message)

class TransitionNameNotUniqueError(ConfigurationError):
    """ Each transition within a machine must have a unique name. """
    def __init__(self, machineName, transitionName):
        """ Initialize exception """
        message = 'Transition names within a machine must be unique. (Machine %s, Transition %s)' % \
                  (machineName, transitionName)
        super().__init__(message)

class InvalidTransitionEventNameError(ConfigurationError):
    """ The transition's event name was invalid. """
    def __init__(self, machineName, fromStateName, eventName):
        """ Initialize exception """
        message = 'Transition event name must match pattern "%s". (Machine %s, State %s, Event %s)' % \
                  (constants.NAME_PATTERN, machineName, fromStateName, eventName)
        super().__init__(message)

class TransitionUnknownToStateError(ConfigurationError):
    """ Each transition must specify a to state. """
    def __init__(self, machineName, transitionName, toState):
        """ Initialize exception """
        message = 'Transition to state is undefined. (Machine %s, Transition %s, To %s)' % \
                  (machineName, transitionName, toState)
        super().__init__(message)

class TransitionToRequiredError(ConfigurationError):
    """ The specified to state is unknown. """
    def __init__(self, machineName, transitionName):
        """ Initialize exception """
        message = '"%s" is required attribute of transition. (Machine %s, Transition %s)' % \
                  (constants.TRANS_TO_ATTRIBUTE, machineName, transitionName)
        super().__init__(message)

class TransitionEventRequiredError(ConfigurationError):
    """ Each transition requires an event to be bound to. """
    def __init__(self, machineName, fromStateName):
        """ Initialize exception """
        message = '"%s" is required attribute of transition. (Machine %s, State %s)' % \
                  (constants.TRANS_EVENT_ATTRIBUTE, machineName, fromStateName)
        super().__init__(message)

class InvalidCountdownError(ConfigurationError):
    """ Countdown must be a positive integer. """
    def __init__(self, countdown, machineName, fromStateName):
        """ Initialize exception """
        message = ('Countdown "%s" must be a positive integer or a dict containing only %s and %s. ' +
                   '(Machine %s, State %s)') % \
                  (countdown, constants.COUNTDOWN_MINIMUM_ATTRIBUTE, constants.COUNTDOWN_MAXIMUM_ATTRIBUTE,
                   machineName, fromStateName)
        super().__init__(message)

class InvalidMachineAttributeError(ConfigurationError):
    """ Unknown machine attributes were found. """
    def __init__(self, machineName, badAttributes):
        """ Initialize exception """
        message = 'The following are invalid attributes a machine: %s. (Machine %s)' % \
                  (badAttributes, machineName)
        super().__init__(message)

class InvalidStateAttributeError(ConfigurationError):
    """ Unknown state attributes were found. """
    def __init__(self, machineName, stateName, badAttributes):
        """ Initialize exception """
        message = 'The following are invalid attributes a state: %s. (Machine %s, State %s)' % \
                  (badAttributes, machineName, stateName)
        super().__init__(message)

class InvalidTransitionAttributeError(ConfigurationError):
    """ Unknown transition attributes were found. """
    def __init__(self, machineName, fromStateName, badAttributes):
        """ Initialize exception """
        message = 'The following are invalid attributes a transition: %s. (Machine %s, State %s)' % \
                  (badAttributes, machineName, fromStateName)
        super().__init__(message)

class InvalidInterfaceError(ConfigurationError):
    """ Interface errors. """
    pass

class InvalidContinuationInterfaceError(InvalidInterfaceError):
    """ The specified state was denoted as a continuation, but it does not have a continuation method. """
    def __init__(self, machineName, stateName):
        message = 'The state was specified as continuation=True, but the action class does not have a ' + \
                  'continuation() method. (Machine {}, State {})'.format(machineName, stateName)
        super().__init__(message)

class InvalidActionInterfaceError(InvalidInterfaceError):
    """ The specified state's action class does not have an execute() method. """
    def __init__(self, machineName, stateName):
        message = 'The state\'s action class does not have an execute() method. (Machine %s, State %s)' % \
                  (machineName, stateName)
        super().__init__(message)

class InvalidEntryInterfaceError(InvalidInterfaceError):
    """ The specified state's entry class does not have an execute() method. """
    def __init__(self, machineName, stateName):
        message = 'The state\'s entry class does not have an execute() method. (Machine %s, State %s)' % \
                  (machineName, stateName)
        super().__init__(message)

class InvalidExitInterfaceError(InvalidInterfaceError):
    """ The specified state's exit class does not have an execute() method. """
    def __init__(self, machineName, stateName):
        message = 'The state\'s exit class does not have an execute() method. (Machine %s, State %s)' % \
                  (machineName, stateName)
        super().__init__(message)

class InvalidFanInError(ConfigurationError):
    """ fan_in must be a positive integer. """
    def __init__(self, machineName, stateName, fanInPeriod):
        """ Initialize exception """
        message = '%s "%s" is invalid. Must be an integer. (Machine %s, State %s)' % \
                  (constants.STATE_FAN_IN_ATTRIBUTE, fanInPeriod, machineName, stateName)
        super().__init__(message)

class InvalidFanInGroupError(ConfigurationError):
    """ fan_in_group must be a string key. """
    def __init__(self, machineName, stateName, fanInGroup):
        """ Initialize exception """
        message = '%s "%s" is invalid. Requires fan_in attribute as well. (Machine %s, State %s)' % \
                  (constants.STATE_FAN_IN_GROUP_ATTRIBUTE, fanInGroup, machineName, stateName)
        super().__init__(message)

class FanInContinuationNotSupportedError(ConfigurationError):
    """ Cannot have fan_in and continuation on the same state, because it hurts our head at the moment. """
    def __init__(self, machineName, stateName):
        """ Initialize exception """
        message = '%s and %s are not supported on the same state. Maybe some day... (Machine %s, State %s)' % \
                  (constants.STATE_CONTINUATION_ATTRIBUTE, constants.STATE_FAN_IN_ATTRIBUTE,
                   machineName, stateName)
        super().__init__(message)

class UnsupportedConfigurationError(ConfigurationError):
    """ Some exit and transition actions are not allowed near fan_in and continuation. At least not at the moment. """
    def __init__(self, machineName, stateName, message):
        """ Initialize exception """
        message = '{} (Machine {}, State {})'.format(message, machineName, stateName)
        super().__init__(message)
