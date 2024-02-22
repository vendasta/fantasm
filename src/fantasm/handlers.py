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
import logging
import sys
import time
import traceback
from urllib.parse import parse_qs

from google.appengine.ext import db, deferred

try:
    from google.appengine.api.capabilities import CapabilitySet
except ImportError:
    CapabilitySet = None
from google.appengine.ext import ndb

from fantasm import config, constants
from fantasm.constants import (EVENT_PARAM, HTTP_REQUEST_HEADER_PREFIX,
                               IMMEDIATE_MODE_PARAM, INSTANCE_NAME_PARAM,
                               MESSAGES_PARAM, NON_CONTEXT_PARAMS,
                               RETRY_COUNT_PARAM, STARTED_AT_PARAM,
                               STATE_PARAM, TASK_NAME_PARAM)
from fantasm.exceptions import (TRANSIENT_ERRORS, FSMRuntimeError,
                                RequiredServicesUnavailableRuntimeError,
                                UnknownMachineError)
from fantasm.fsm import FSM
from fantasm.lock import RunOnceSemaphore
from fantasm.models import Encoder, _FantasmFanIn
from fantasm.utils import NoOpQueue

REQUIRED_SERVICES = ("memcache", "datastore_v3", "taskqueue")


class TemporaryStateObject(dict):
    """A simple object that is passed throughout a machine dispatch that can hold temporary
    in-flight data.
    """

    pass


def getMachineNameFromRequest(environ):
    """Returns the machine name embedded in the request.

    @param environ: the WSGI environment
    @return: the machineName (as a string)
    """
    path = environ["PATH_INFO"]

    # strip off the mount-point
    currentConfig = config.currentConfiguration()
    mountPoint = currentConfig.rootUrl  # e.g., '/fantasm/'
    if not path.startswith(mountPoint):
        raise FSMRuntimeError("rootUrl '%s' must match app.yaml mapping." % mountPoint)
    path = path[len(mountPoint) :]

    # split on '/', the second item will be the machine name
    parts = path.split("/")
    return parts[1]  # 0-based index


def getMachineConfig(environ):
    """Returns the machine configuration specified by a URI in a HttpReuest

    @param environ: the WSGI environment
    @return: a config._machineConfig instance
    """

    # parse out the machine-name from the path {mount-point}/fsm/{machine-name}/startState/event/endState/
    # NOTE: /startState/event/endState/ is optional
    machineName = getMachineNameFromRequest(environ)

    # load the configuration, lookup the machine-specific configuration
    # FIXME: sort out a module level cache for the configuration - it must be sensitive to YAML file changes
    # for developer-time experience
    currentConfig = config.currentConfiguration()
    try:
        machineConfig = currentConfig.machines[machineName]
        return machineConfig
    except KeyError:
        raise UnknownMachineError(machineName)


class FSMLogHandler:
    """The handler used for logging"""

    def __call__(self, environ, start_response):
        """Runs the serialized function"""
        if environ["REQUEST_METHOD"] == "POST":
            body = environ["wsgi.input"].read().encode('latin1')
            deferred.run(body)
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b""]


class FSMFanInCleanupHandler:
    """The handler used for logging"""

    def __call__(self, environ, start_response):
        """Runs the serialized function"""
        if environ["REQUEST_METHOD"] == "POST":
            body = environ["wsgi.input"].read()
            workIndex = parse_qs(body).get(constants.WORK_INDEX_PARAM, [None])[0]
            q = _FantasmFanIn.all(namespace="").filter("workIndex =", workIndex)
            db.delete(q)
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b""]


class FSMHandler:
    """The main worker handler, used to process queued machine events."""

    def getCurrentFSM(self):
        """Returns the current FSM singleton."""
        if not hasattr(self, "_fsm"):
            currentConfig = config.currentConfiguration()
            setattr(self, "_fsm", FSM(currentConfig=currentConfig))
        return getattr(self, "_fsm")

    @ndb.toplevel
    def __call__(self, environ, start_response):
        """CALL"""
        method = environ["REQUEST_METHOD"]
        if method not in ("GET", "POST"):
            start_response("405 Method Not Allowed", [("Content-Type", "text/plain")])
            return [b"Method Not Allowed"]
        try:
            result = self.get_or_post(environ, start_response)
            if not result:
                start_response("200 OK", [("Content-Type", "text/plain")])
                return [b""]
            return result
        except Exception as e:
            self.handle_exception(e)
            start_response(
                "500 Internal Server Error", [("Content-Type", "text/plain")]
            )
            raise e

    def handle_exception(self, exception):
        """Delegates logging to the FSMContext logger"""
        logger = logging
        currentFsm = self.getCurrentFSM()
        if currentFsm:
            logger = currentFsm.logger
        level = logger.error
        if exception.__class__ in TRANSIENT_ERRORS:
            level = logger.warn
        lines = "".join(traceback.format_exception(*sys.exc_info()))
        level("FSMHandler caught Exception\n" + lines)

    def get_or_post(self, environ, start_response):
        """Handles the GET/POST request.

        FIXME: this is getting a touch long
        """

        # ensure that we have our services for the next 30s (length of a single request)
        if config.currentConfiguration().enableCapabilitiesCheck:
            unavailable = set()
            for service in REQUIRED_SERVICES:
                try:
                    if not CapabilitySet(service).is_enabled():
                        unavailable.add(service)
                except Exception:
                    # Something failed while checking capabilities, just assume they are going to be available.
                    # These checks were from an era of lower-reliability which is no longer the case.
                    pass
            if unavailable:
                raise RequiredServicesUnavailableRuntimeError(unavailable)

        # the case of headers is inconsistent on dev_appserver and appengine
        # ie 'X-AppEngine-TaskRetryCount' vs. 'X-AppEngine-Taskretrycount'
        lowerCaseHeaders = {k[5:].lower(): v for k, v in environ.items() if k.startswith('HTTP_')}

        taskName = lowerCaseHeaders.get("x-appengine-taskname")
        retryCount = int(lowerCaseHeaders.get("x-appengine-taskretrycount", 0))

        # pull out X-Fantasm-* headers
        headers = None
        for key, value in list(lowerCaseHeaders.items()):
            if key.startswith(HTTP_REQUEST_HEADER_PREFIX.lower()):
                headers = headers or {}
                if "," in value:
                    headers[key] = [v.strip() for v in value.split(",")]
                else:
                    headers[key] = value.strip()

        method = environ["REQUEST_METHOD"]
        if method == "POST":
            request_body = environ["wsgi.input"].read()
            requestData = parse_qs(request_body)
        if method == "GET":
            requestData = parse_qs(environ["QUERY_STRING"])
        method = requestData.get("method") or method

        machineName = getMachineNameFromRequest(environ)

        # get the incoming instance name, if any
        instanceName = requestData.get(INSTANCE_NAME_PARAM, [None])[0]

        # get the incoming state, if any
        fsmState = requestData.get(STATE_PARAM, [None])[0]

        # get the incoming event, if any
        fsmEvent = requestData.get(EVENT_PARAM, [None])[0]

        assert (
            fsmState and instanceName
        ) or True  # if we have a state, we should have an instanceName
        assert (
            fsmState and fsmEvent
        ) or True  # if we have a state, we should have an event

        obj = TemporaryStateObject()

        # make a copy, add the data
        fsm = self.getCurrentFSM().createFSMInstance(
            machineName,
            currentStateName=fsmState,
            instanceName=instanceName,
            method=method,
            obj=obj,
            headers=headers,
        )

        # Taskqueue can invoke multiple tasks of the same name occassionally. Here, we'll use
        # a datastore transaction as a semaphore to determine if we should actually execute this or not.
        if taskName and fsm.useRunOnceSemaphore:
            semaphoreKey = "{}--{}".format(taskName, retryCount)
            semaphore = RunOnceSemaphore(semaphoreKey, None)
            if not semaphore.writeRunOnceSemaphore(payload="fantasm")[0]:
                # we can simply return here, this is a duplicate fired task
                logging.warn(
                    'A duplicate task "%s" has been queued by taskqueue infrastructure. Ignoring.',
                    taskName,
                )
                return

        # in "immediate mode" we try to execute as much as possible in the current request
        # for the time being, this does not include things like fork/spawn/contuniuations/fan-in
        immediateMode = IMMEDIATE_MODE_PARAM in list(requestData.keys())
        if immediateMode:
            obj[IMMEDIATE_MODE_PARAM] = immediateMode
            obj[MESSAGES_PARAM] = []
            fsm.Queue = NoOpQueue  # don't queue anything else

        # pull all the data off the url and stuff into the context
        for key, value in list(requestData.items()):
            if key in NON_CONTEXT_PARAMS:
                continue  # these are special, don't put them in the data

            # deal with ...a=1&a=2&a=3...
            value = requestData.get(key, None)

            if len(value) == 1 and not str(key).endswith('[]'):
                value = value[0]
            if str(key).endswith('[]'):
                key = key[:-2]

            if key in list(fsm.contextTypes.keys()):
                fsm.putTypedValue(key, value)
            else:
                fsm[key] = value

        if not (fsmState or fsmEvent):

            # just queue up a task to run the initial state transition using retries
            fsm[STARTED_AT_PARAM] = time.time()

            # initialize the fsm, which returns the 'pseudo-init' event
            fsmEvent = fsm.initialize()

        else:

            # add the retry counter into the machine context from the header
            obj[RETRY_COUNT_PARAM] = retryCount

            # add the actual task name to the context
            obj[TASK_NAME_PARAM] = taskName

            # dispatch and return the next event
            fsmEvent = fsm.dispatch(fsmEvent, obj)

        # loop and execute until there are no more events - any exceptions
        # will make it out to the user in the response - useful for debugging
        if immediateMode:
            while fsmEvent:
                fsmEvent = fsm.dispatch(fsmEvent, obj)

            start_response("200 OK", [("Content-Type", "application/json")])
            data = {
                "obj": obj,
                "context": fsm,
            }
            return [json.dumps(data, cls=Encoder).encode("utf-8")]
