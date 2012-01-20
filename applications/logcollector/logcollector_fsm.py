""" log collector

This application consists of a single state machine that runs as a continuation
over App Engine logs fetched via the Log Reader API. The continuation token is
the end_time parameter returned by logservice.fetch. When no logs are available,
the fantasm infrastructure raises and re-tries the Task, backing off up to a 
maximum of 60 seconds.

"""
import time
import pickle

from fantasm.action import FSMAction
from fantasm.action import ContinuationFSMAction

from google.appengine.api import logservice
from google.appengine.api import mail

LOG_REQUESTS_PARAM = 'log-requests'
MINIMUM_LOG_LEVEL_PARAM = 'minimum-log-level'
INCLUDE_INCOMPLETE_PARAM = 'include-incomplete'
INCLUDE_APP_LOGS_PARAM = 'include-app-logs'
VERSION_IDS_PARAM = 'version-ids'
BATCH_SIZE_PARAM = 'batch-size'
START_TIME_PARAM = 'start-time'

SENDER_EMAIL_PARAM = 'sender'
TO_EMAIL_PARAM = 'to'
SUBJECT_PREFIX_PARAM = 'subject'

OK = 'ok'

# defaults different than logservice.fetch defaults
DEFAULT_MINIMUM_LOG_LEVEL = logservice.LOG_LEVEL_CRITICAL
DEFAULT_INCLUDE_APP_LOGS = True
LOG_LEVEL_DESCRIPTION = {
    logservice.LOG_LEVEL_DEBUG: 'logservice.LOG_LEVEL_DEBUG',
    logservice.LOG_LEVEL_INFO: 'logservice.LOG_LEVEL_INFO',
    logservice.LOG_LEVEL_WARNING: 'logservice.LOG_LEVEL_WARNING', 
    logservice.LOG_LEVEL_ERROR: 'logservice.LOG_LEVEL_ERROR',
    logservice.LOG_LEVEL_CRITICAL: 'logservice.LOG_LEVEL_CRITICAL'
}

class NoLogsAvailableException( Exception ):
    """ Exception raised when logservice.fetch returns nothing """
    pass

class InvalidVersionException( Exception ):
    """ Exception raised when trying to monitor self """
    pass

class SetStartTime( FSMAction ):
    """ Sets initial start_time """
    
    def execute(self, context, obj):
        """ Sets initial start_time 
        
        @param context: an FSMContext instance
        @param obj: a temporary state dictionary 
        """
        context[START_TIME_PARAM] = time.time()
        return OK

class CollectLogs( ContinuationFSMAction ):
    """ Collects AppEngine logs """
    
    def continuation(self, context, obj, token=None):
        """ Continuation - see ContinuationFSMAction.continuation(...)
        
        @param context: an FSMContext instance
        @param obj: a temporary state dictionary 
        """
        if not token:
            startTime = context[START_TIME_PARAM] # need a recent start_time, or an F1 instance runs out of memory
        else:
            startTime = pickle.loads(str(token))
        
        offset = None
        endTime = None
        minimumLogLevel = context.get(MINIMUM_LOG_LEVEL_PARAM, DEFAULT_MINIMUM_LOG_LEVEL)
        includeIncomplete = context.get(INCLUDE_INCOMPLETE_PARAM, False)
        includeAppLogs = context.get(INCLUDE_APP_LOGS_PARAM, DEFAULT_INCLUDE_APP_LOGS)
        versionIds = context.get(VERSION_IDS_PARAM)
        if versionIds and not isinstance(versionIds, list):
            versionIds = [versionIds]
        batchSize = context.get(BATCH_SIZE_PARAM, None)
        
        logRequestsIter = logservice.fetch(start_time=startTime, 
                                           end_time=endTime, 
                                           offset=offset, 
                                           minimum_log_level=minimumLogLevel, 
                                           include_incomplete=includeIncomplete, 
                                           include_app_logs=includeAppLogs, 
                                           version_ids=versionIds, 
                                           batch_size=batchSize)
        
        logRequests = list(logRequestsIter)
        logRequests = [l for l in logRequests if l.end_time > startTime] # fetch does no strictly obey start_time
        
        # This effectively implements polling with exponential backoff, up to a
        # maximum of 60s, as defined in fsm.yaml.
        if not logRequests:
            raise NoLogsAvailableException()
        
        obj[LOG_REQUESTS_PARAM] = logRequests
        
        return pickle.dumps(logRequests[0].end_time) # pickle the float to avoid loss of precision
    
    def handleLogRequests(self, context, obj):
        """ Handles the logs. Currently just send an email to admin
        
        @param context: an FSMContext instance
        @param obj: a temporary state dictionary 
        """
        logRequests = obj.get(LOG_REQUESTS_PARAM)
        if logRequests:
            sender = context.get(SENDER_EMAIL_PARAM)
            to = context.get(TO_EMAIL_PARAM)
            if sender and to:
                minimumLogLevel = context.get(MINIMUM_LOG_LEVEL_PARAM, DEFAULT_MINIMUM_LOG_LEVEL)
                for logRequest in logRequests:
                    subjectPrefix = context.get(SUBJECT_PREFIX_PARAM, '')
                    subject = 'LogCollector detected LEVEL=%s on PATH=%s' % \
                                  (LOG_LEVEL_DESCRIPTION.get(minimumLogLevel, 'logservice.LOG_LEVEL_UNKNOWN'),
                                   logRequest.resource)
                    body = '\n'.join([appLog.message for appLog in logRequest.app_logs]) or 'empty'
                    mail.send_mail(sender, to, subjectPrefix + subject, body)
        
    def execute(self, context, obj):
        """ Delegates to self.handleLogRequests
        
        @param context: an FSMContext instance
        @param obj: a temporary state dictionary 
        """
        return self.handleLogRequests(context, obj)
