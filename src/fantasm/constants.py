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

import json
import os
import re

# these parameters are not stored in the FSMContext, but are used to drive the fantasm task/event dispatching mechanism
STATE_PARAM = '__st__'
EVENT_PARAM = '__ev__'
INSTANCE_NAME_PARAM = '__in__'
TERMINATED_PARAM = '__tm__'
TASK_NAME_PARAM = '__tn__'
FAN_IN_RESULTS_PARAM = '__fi__'
RETRY_COUNT_PARAM = '__rc__'
FORKED_CONTEXTS_PARAM = '__fc__'
IMMEDIATE_MODE_PARAM = '__im__'
MESSAGES_PARAM = '__ms__'
FANNED_IN_CONTEXT = '__fic__'
NON_CONTEXT_PARAMS = (STATE_PARAM, EVENT_PARAM, INSTANCE_NAME_PARAM, TERMINATED_PARAM, TASK_NAME_PARAM,
                      FAN_IN_RESULTS_PARAM, RETRY_COUNT_PARAM, FORKED_CONTEXTS_PARAM, IMMEDIATE_MODE_PARAM,
                      MESSAGES_PARAM, FANNED_IN_CONTEXT)


# these parameters are stored in the FSMContext, and used to drive the task naming machanism
STEPS_PARAM = '__step__' # tracks the number of steps executed in the machine so far
CONTINUATION_PARAM = '__ct__' # tracks the continuation token (for continuation states)
GEN_PARAM = '__ge__' # used to uniquify the machine instance names (for continuations and spawns)
INDEX_PARAM = '__ix__'
WORK_INDEX_PARAM = '__wix__'
FORK_PARAM = '__fk__'
STARTED_AT_PARAM = '__sa__'
FAN_IN_GROUP_PARAM = '__fig__'
CONTINUATION_RESULTS_COUNTER_PARAM = '__crc__'
CONTINUATION_COMPLETE_PARAM = '__cc__'
CONTINUATION_RESULTS_SIZE_PARAM = '__crs__'
CONTEXT_PARAMS = (STEPS_PARAM, CONTINUATION_PARAM, GEN_PARAM, INDEX_PARAM, WORK_INDEX_PARAM,
                  FORK_PARAM, STARTED_AT_PARAM, FAN_IN_GROUP_PARAM, CONTINUATION_RESULTS_COUNTER_PARAM,
                  CONTINUATION_COMPLETE_PARAM)

PRIVATE_PARAMS = set(NON_CONTEXT_PARAMS) | set(CONTEXT_PARAMS)

# this dict is used for casting strings in HttpRequest.GET to the appropriate type to put into FSMContext
PARAM_TYPES = {
    STEPS_PARAM : int,
    GEN_PARAM : json.loads,
    INDEX_PARAM: int,
    FORK_PARAM: int,
    STARTED_AT_PARAM: float,
    CONTINUATION_RESULTS_COUNTER_PARAM: int,
    CONTINUATION_COMPLETE_PARAM: bool,
    CONTINUATION_RESULTS_SIZE_PARAM: int,
}

CHARS_FOR_RANDOM = 'BDGHJKLMNPQRTVWXYZ23456789' # no vowels or things that look like vowels - profanity-free!

# these are part of the continuation interface, and are generally used as
# keys in the temporary state object 'obj'. this is whey they are not included
# above in the __var__ settings
CONTINUATION_RESULTS_KEY = 'results'
CONTINUATION_RESULTS_PARAM = CONTINUATION_RESULTS_KEY
CONTINUATION_RESULT_KEY = 'result'
CONTINUATION_RESULT_PARAM = CONTINUATION_RESULT_KEY
CONTINUATION_MORE_RESULTS_KEY = 'has_more_results'

REQUEST_LENGTH = 30

MAX_NAME_LENGTH = 50 # we need to combine a number of names into a task name, which has a 500 char limit
NAME_PATTERN = r'^[a-zA-Z0-9-]{1,%s}$' % MAX_NAME_LENGTH
NAME_RE = re.compile(NAME_PATTERN)

HTTP_REQUEST_HEADER_PREFIX = 'X-Fantasm-'
HTTP_ENVIRON_KEY_PREFIX = 'HTTP_X_FANTASM_'
HTTP_REQUEST_HEADER_QUEUENAME = HTTP_REQUEST_HEADER_PREFIX + 'Queuename'

DEFAULT_TASK_RETRY_LIMIT = None
DEFAULT_MIN_BACKOFF_SECONDS = None
DEFAULT_MAX_BACKOFF_SECONDS = None
DEFAULT_TASK_AGE_LIMIT = None
DEFAULT_MAX_DOUBLINGS = None
DEFAULT_QUEUE_NAME = 'default'
DEFAULT_LOG_QUEUE_NAME = DEFAULT_QUEUE_NAME
DEFAULT_CLEANUP_QUEUE_NAME = DEFAULT_QUEUE_NAME
DEFAULT_TARGET = None
DEFAULT_USE_RUN_ONCE_SEMAPHORE = True

NO_FAN_IN = -1
DEFAULT_FAN_IN_PERIOD = NO_FAN_IN # fan_in period (in seconds)
DATASTORE_ASYNCRONOUS_INDEX_WRITE_WAIT_TIME = 5.0 # seconds

DEFAULT_COUNTDOWN = 0

YAML_NAMES = ('fsm.yaml', 'fsm.yml', 'fantasm.yaml', 'fantasm.yml')

DEFAULT_ROOT_URL = '/fantasm/' # where all the fantasm handlers are mounted
DEFAULT_LOG_URL = '/fantasm/log/'
DEFAULT_CLEANUP_URL = '/fantasm/cleanup/'
DEFAULT_ENABLE_CAPABILITIES_CHECK = True

### attribute names for YAML parsing

IMPORT_ATTRIBUTE = 'import'

NAMESPACE_ATTRIBUTE = 'namespace'
QUEUE_NAME_ATTRIBUTE = 'queue'
TARGET_ATTRIBUTE = 'target'
COUNTDOWN_ATTRIBUTE = 'countdown'
COUNTDOWN_MINIMUM_ATTRIBUTE = 'minimum'
COUNTDOWN_MAXIMUM_ATTRIBUTE = 'maximum'
MAX_RETRIES_ATTRIBUTE = 'max_retries' # deprecated, use task_retry_limit instead
TASK_RETRY_LIMIT_ATTRIBUTE = 'task_retry_limit'
MIN_BACKOFF_SECONDS_ATTRIBUTE = 'min_backoff_seconds'
MAX_BACKOFF_SECONDS_ATTRIBUTE = 'max_backoff_seconds'
TASK_AGE_LIMIT_ATTRIBUTE = 'task_age_limit'
MAX_DOUBLINGS_ATTRIBUTE = 'max_doublings'

ROOT_URL_ATTRIBUTE = 'root_url'
ENABLE_CAPABILITIES_CHECK_ATTRIBUTE = 'enable_capabilities_check'
STATE_MACHINES_ATTRIBUTE = 'state_machines'

MACHINE_NAME_ATTRIBUTE = 'name'
MACHINE_STATES_ATTRIBUTE = 'states'
MACHINE_TRANSITIONS_ATTRIBUTE = 'transitions'
MACHINE_CONTEXT_TYPES_ATTRIBUTE = 'context_types'
MACHINE_LOGGING_NAME_ATTRIBUTE = 'logging'
MACHINE_USE_RUN_ONCE_SEMAPHORE_ATTRIBUTE = 'use_run_once_semaphore'
VALID_MACHINE_ATTRIBUTES = (NAMESPACE_ATTRIBUTE, MAX_RETRIES_ATTRIBUTE, TASK_RETRY_LIMIT_ATTRIBUTE,
                            MIN_BACKOFF_SECONDS_ATTRIBUTE, MAX_BACKOFF_SECONDS_ATTRIBUTE,
                            TASK_AGE_LIMIT_ATTRIBUTE, MAX_DOUBLINGS_ATTRIBUTE,
                            MACHINE_NAME_ATTRIBUTE, QUEUE_NAME_ATTRIBUTE, TARGET_ATTRIBUTE,
                            MACHINE_STATES_ATTRIBUTE, MACHINE_CONTEXT_TYPES_ATTRIBUTE,
                            MACHINE_LOGGING_NAME_ATTRIBUTE, MACHINE_USE_RUN_ONCE_SEMAPHORE_ATTRIBUTE,
                            COUNTDOWN_ATTRIBUTE)
                            # MACHINE_TRANSITIONS_ATTRIBUTE is intentionally not in this list;
                            # it is used internally only

LOGGING_DEFAULT = 'default'
LOGGING_PERSISTENT = 'persistent'
VALID_LOGGING_VALUES = (LOGGING_DEFAULT, LOGGING_PERSISTENT)

STATE_NAME_ATTRIBUTE = 'name'
STATE_ENTRY_ATTRIBUTE = 'entry'
STATE_EXIT_ATTRIBUTE = 'exit'
STATE_ACTION_ATTRIBUTE = 'action'
STATE_INITIAL_ATTRIBUTE = 'initial'
STATE_FINAL_ATTRIBUTE = 'final'
STATE_CONTINUATION_ATTRIBUTE = 'continuation'
STATE_CONTINUATION_COUNTDOWN_ATTRIBUTE = 'continuation_countdown'
STATE_FAN_IN_ATTRIBUTE = 'fan_in'
STATE_FAN_IN_GROUP_ATTRIBUTE = 'fan_in_group'
STATE_TRANSITIONS_ATTRIBUTE = 'transitions'
VALID_STATE_ATTRIBUTES = (NAMESPACE_ATTRIBUTE, STATE_NAME_ATTRIBUTE, STATE_ENTRY_ATTRIBUTE, STATE_EXIT_ATTRIBUTE,
                          STATE_ACTION_ATTRIBUTE, STATE_INITIAL_ATTRIBUTE, STATE_FINAL_ATTRIBUTE,
                          STATE_CONTINUATION_ATTRIBUTE, STATE_FAN_IN_ATTRIBUTE, STATE_FAN_IN_GROUP_ATTRIBUTE,
                          STATE_TRANSITIONS_ATTRIBUTE, STATE_CONTINUATION_COUNTDOWN_ATTRIBUTE)

TRANS_TO_ATTRIBUTE = 'to'
TRANS_EVENT_ATTRIBUTE = 'event'
TRANS_ACTION_ATTRIBUTE = 'action'
VALID_TRANS_ATTRIBUTES = (NAMESPACE_ATTRIBUTE, MAX_RETRIES_ATTRIBUTE, TASK_RETRY_LIMIT_ATTRIBUTE,
                          MIN_BACKOFF_SECONDS_ATTRIBUTE, MAX_BACKOFF_SECONDS_ATTRIBUTE,
                          TASK_AGE_LIMIT_ATTRIBUTE, MAX_DOUBLINGS_ATTRIBUTE,
                          TRANS_TO_ATTRIBUTE, TRANS_EVENT_ATTRIBUTE, TRANS_ACTION_ATTRIBUTE,
                          COUNTDOWN_ATTRIBUTE, QUEUE_NAME_ATTRIBUTE, TARGET_ATTRIBUTE)

DEV_APPSERVER = 'SERVER_SOFTWARE' in os.environ and os.environ['SERVER_SOFTWARE'].find('Development') >= 0
