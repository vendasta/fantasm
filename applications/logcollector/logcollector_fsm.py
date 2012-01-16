""" log collector

"""
import logging
import time

import fantasm

from google.appengine.api import logservice
from google.appengine.api import mail

LOG_REQUESTS_PARAM = 'log-requests'
MINIMUM_LOG_LEVEL_PARAM = 'minimum-log-level'
INCLUDE_INCOMPLETE_PARAM = 'include-incomplete'
INCLUDE_APP_LOGS_PARAM = 'include-app-logs'
VERSION_IDS_PARAM = 'version-ids'
BATCH_SIZE_PARAM = 'batch-size'
START_TIME_PARAM = 'start-time'

EMAIL_PARAM = 'email'

OK = 'ok'

class NoLogsAvailableException( Exception ):
    """ Exception raised when logservice.fetch returns nothing """
    pass

class SetStartTime( fantasm.action.FSMAction ):
    """ Sets initial start_time """
    
    def execute(self, context, obj):
        """ Sets initial start_time 
        
        @param context: an FSMContext instance
        @param obj: a temporary state dictionary 
        """
        context[START_TIME_PARAM] = time.time()
        return OK

class CollectLogs( fantasm.action.ContinuationFSMAction ):
    """ Collects AppEngine logs """
    
    def continuation(self, context, obj, token=None):
        """ Continuation - see ContinuationFSMAction.continuation(...)
        
        @param context: an FSMContext instance
        @param obj: a temporary state dictionary 
        """
        if not token:
            offset = None
        else:
            offset = token
        
        startTime = context[START_TIME_PARAM]
        endTime = None
        minimumLogLevel = context.get(MINIMUM_LOG_LEVEL_PARAM)
        includeIncomplete = context.get(INCLUDE_INCOMPLETE_PARAM, False)
        includeAppLogs = context.get(INCLUDE_APP_LOGS_PARAM, True)
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
        
        if not logRequests:
            raise NoLogsAvailableException()
        
        obj[LOG_REQUESTS_PARAM] = logRequests
        
        return logRequests[0].offset
        
    def execute(self, context, obj):
        """ Handles the logs. Currently just send an email to admin
        
        @param context: an FSMContext instance
        @param obj: a temporary state dictionary 
        """
        
        logRequests = obj.get(LOG_REQUESTS_PARAM)
        if logRequests:
            sender = to = context.get(EMAIL_PARAM)
            if sender:
                for logRequest in logRequests:
                    subject = 'LogCollector detected messages matching threshold on PATH=' + logRequest.resource
                    body = '\n'.join([appLog.message for appLog in logRequest.app_logs]) or 'empty'
                    mail.send_mail(sender, to, subject, body)
        
        

