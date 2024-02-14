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

from fantasm.constants import CONTINUATION_RESULTS_KEY
from fantasm.constants import CONTINUATION_RESULT_KEY
from fantasm.constants import CONTINUATION_MORE_RESULTS_KEY
from fantasm.constants import CONTINUATION_RESULTS_COUNTER_PARAM
from fantasm.constants import CONTINUATION_RESULTS_SIZE_PARAM
from fantasm.constants import CONTINUATION_COMPLETE_PARAM
from fantasm.constants import STEPS_PARAM
from fantasm.constants import GEN_PARAM

class FSMAction:
    """ Defines the interface for all user actions. """

    def execute(self, context, obj):
        """ Executes some action. The return value is ignored, _except_ for the main state action.

        @param context The FSMContext (i.e., machine). context.get() and context.put() can be used to get data
                       from/to the context.
        @param obj: An object which the action can operate on

        For the main state action, the return value should be a string representing the event to be dispatched.
        Actions performed should be careful to be idempotent: because of potential retry mechanisms
        (notably with TaskQueueFSMContext), individual execute methods may get executed more than once with
        exactly the same context.
        """
        raise NotImplementedError()

class ContinuationFSMAction(FSMAction):
    """ Defines the interface for all continuation actions. """

    def continuation(self, context, obj, token=None):
        """ Accepts a token (may be None) and returns the next token for the continutation.

        @param token: the continuation token
        @param context The FSMContext (i.e., machine). context.get() and context.put() can be used to get data
                       from/to the context.
        @param obj: An object which the action can operate on
        """
        raise NotImplementedError()

    @staticmethod
    def checkFanInForTotalResultsCount(contexts, obj):
        """ Checks for the total number of continuation results

        @param contexts: a list of FSMContext instances, from a fan-in
        @param obj: An object which the action can operate on
        @return: the total number of results from the continuation, or None if that is not yet available

        FIXME: this currently only handles single-level continuations
        """
        totalResultsCount = None
        for context in contexts:
            if context.get(CONTINUATION_COMPLETE_PARAM):
                totalResultsCount = context[CONTINUATION_RESULTS_COUNTER_PARAM]
        return totalResultsCount

    @staticmethod
    def getResultsCount(context, obj):
        """ Returns the number of continuation results from the current batch

        @param token: the continuation token
        @param context The FSMContext (i.e., machine). context.get() and context.put() can be used to get data
                       from/to the context.
        @return: the number of continuation results in this batch

        FIXME: this currently only handles single-level continuations
        """
        return context.get(CONTINUATION_RESULTS_SIZE_PARAM, 0)

class DatastoreContinuationFSMAction(ContinuationFSMAction):
    """ A datastore continuation. """

    __NEXT_TOKEN = '__next_token__'

    def continuation(self, context, obj, token=None):
        """ Accepts a token (an optional cursor) and returns the next token for the continutation.
        The results of the query are stored on obj.results.
        """
        limit = self.getBatchSize(context, obj)
        results = self._fetchResults(limit, context, obj, token=token)

        # place results on obj.results
        obj[CONTINUATION_RESULTS_KEY] = results
        obj.results = obj[CONTINUATION_RESULTS_KEY] # deprecated interface

        # add first obj.results item on obj.result - convenient for batch size 1
        if obj[CONTINUATION_RESULTS_KEY] and len(obj[CONTINUATION_RESULTS_KEY]) > 0:
            obj[CONTINUATION_RESULT_KEY] = obj[CONTINUATION_RESULTS_KEY][0]
        else:
            obj[CONTINUATION_RESULT_KEY] = None
        obj.result = obj[CONTINUATION_RESULT_KEY] # deprecated interface

        context[CONTINUATION_RESULTS_COUNTER_PARAM] = \
            context.get(GEN_PARAM, {}).get(str(context[STEPS_PARAM]), 0) * limit + len(obj[CONTINUATION_RESULTS_KEY])
        context[CONTINUATION_RESULTS_SIZE_PARAM] = len(obj[CONTINUATION_RESULTS_KEY])

        if len(obj[CONTINUATION_RESULTS_KEY]) == limit:
            nextToken = self._getNextToken(context, obj, token=token)
            if nextToken:
                context[CONTINUATION_COMPLETE_PARAM] = False
            else:
                context[CONTINUATION_COMPLETE_PARAM] = True
            return nextToken
        else:
            context[CONTINUATION_COMPLETE_PARAM] = True
        return None

    def _fetchResults(self, limit, context, obj, token=None):
        """ Actually fetches the results. """
        query = self.getQuery(context, obj)
        if token:
            query.with_cursor(token)
        results = query.fetch(limit)
        obj[self.__NEXT_TOKEN] = query.cursor()
        return results

    def _getNextToken(self, context, obj, token=None):
        """ Gets the next token. """
        return obj.pop(self.__NEXT_TOKEN)

    def getQuery(self, context, obj):
        """ Returns a GqlQuery """
        raise NotImplementedError()

    # W0613: 78:DatastoreContinuationFSMAction.getBatchSize: Unused argument 'obj'
    def getBatchSize(self, context, obj): # pylint: disable=W0613
        """ Returns a batch size, default 1. Override for different values. """
        return 1

class NDBDatastoreContinuationFSMAction(DatastoreContinuationFSMAction):
    """ A datastore continuation, using the NDB API.

    IMPORTANT!!!

    To use this effectively, you must uncomment some lines in handlers.py. Search for "ndb_context" to see
    the specific lines to uncomment (there are 3). If you do not do this, errors in your own code may end up
    surfacing as:

        Deadlock waiting for <Future 94c0fd8731c2dca0 created by tasklet_wrapper(tasklets.py:906)
        for tasklet positional_wrapper(datastore_rpc.py:84); pending>

    with seemingly unrelated stacktraces. These are very difficult to debug. Uncommenting the lines in
    handlers.py will give you nice stacktraces again.
    """

    __NEXT_TOKEN = '__next_token__'

    def _fetchResults(self, limit, context, obj, token=None):
        """ Actually fetches the results. """
        from google.appengine.ext.ndb import query as ndb_query

        query = self.getQuery(context, obj)
        assert isinstance(query, ndb_query.Query)

        kwargs = {
            'produce_cursors': True,
            'keys_only': self.getKeysOnly(context, obj),
            'deadline': self.getDeadline(context, obj),
            'read_policy': self.getReadPolicy(context, obj),
        }

        if token:
            kwargs['start_cursor'] = ndb_query.Cursor.from_websafe_string(token)

        results, cursor, more = query.fetch_page(limit, **kwargs)

        obj[CONTINUATION_MORE_RESULTS_KEY] = more
        obj[self.__NEXT_TOKEN] = more and cursor.to_websafe_string() or None
        return results

    def _getNextToken(self, context, obj, token=None):
        """ Gets the next token. """
        return obj.pop(self.__NEXT_TOKEN)

    def getQuery(self, context, obj):
        """ Returns a google.appengine.ext.ndb.query.Query object. """
        raise NotImplementedError()

    # W0613: 78:DatastoreContinuationFSMAction.getBatchSize: Unused argument 'obj'
    def getKeysOnly(self, context, obj): # pylint: disable=W0613
        """ Returns if the query should returns keys_only. Default False. """
        return False

    # W0613: 78:DatastoreContinuationFSMAction.getBatchSize: Unused argument 'obj'
    def getDeadline(self, context, obj): # pylint: disable=W0613
        """ Returns the RPC deadline. Default 5 seconds."""
        return 5

    # W0613: 78:DatastoreContinuationFSMAction.getBatchSize: Unused argument 'obj'
    def getReadPolicy(self, context, obj): # pylint: disable=W0613
        """ Returns the RPC deadline. Default 5 seconds."""
        return 0    # Strong Consistency

class ListContinuationFSMAction(ContinuationFSMAction):
    """ A list-of-things continuation. """

    def getList(self, context, obj):
        """ Returns a list of items to continue over. THIS LIST CANNOT CHANGE BETWEEN CALLS!!!"""
        raise NotImplementedError()

    def getBatchSize(self, context, obj): # pylint: disable=W0613
        """ Returns a batch size, default 1. Override for different values. """
        return 1

    def continuation(self, context, obj, token=None):
        """ Accepts a token (an optional index) and returns the next token for the continutation.
        The results of getList()[token] are stored on obj.results.
        """
        # the token is the index into the list
        items = self.getList(context, obj)
        index = int(token or '0')
        batchsize = limit = self.getBatchSize(context, obj)
        results = items[index:index + batchsize]

        # place results on obj.results
        obj[CONTINUATION_RESULTS_KEY] = results
        obj.results = obj[CONTINUATION_RESULTS_KEY] # deprecated interface'

        # add first obj.results item on obj.result - convenient for batch size 1
        if obj[CONTINUATION_RESULTS_KEY] and len(obj[CONTINUATION_RESULTS_KEY]) > 0:
            obj[CONTINUATION_RESULT_KEY] = obj[CONTINUATION_RESULTS_KEY][0]
        else:
            obj[CONTINUATION_RESULT_KEY] = None
        obj.result = obj[CONTINUATION_RESULT_KEY] # deprecated interface

        index += batchsize

        context[CONTINUATION_RESULTS_COUNTER_PARAM] = \
            context.get(GEN_PARAM, {}).get(str(context[STEPS_PARAM]), 0) * limit + len(obj[CONTINUATION_RESULTS_KEY])
        context[CONTINUATION_COMPLETE_PARAM] = len(obj[CONTINUATION_RESULTS_KEY]) < limit
        context[CONTINUATION_RESULTS_SIZE_PARAM] = len(obj[CONTINUATION_RESULTS_KEY])

        # unlike a datastore continuation, we know when the end of the data
        # occurs by the value of the token.
        if index < len(items):
            return str(index)
