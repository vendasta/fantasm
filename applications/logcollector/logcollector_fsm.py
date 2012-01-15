""" log collector

"""
import logging

import fantasm

from google.appengine.api import logservice

LOG_REQUESTS_PARAM = 'log-requests'
MINIMUM_LOG_LEVEL_PARAM = 'minimum-log-level'
INCLUDE_INCOMPLETE_PARAM = 'include-incomplete'
INCLUDE_APP_LOGS_PARAM = 'include-app-logs'
VERSION_IDS_PARAM = 'version-ids'
BATCH_SIZE_PARAM = 'batch-size'

class Collect( fantasm.action.ContinuationFSMAction ):
    """ Collects AppEngine logs """
    
    def continuation(self, context, obj, token=None):
        """ Continuation """
        if not token:
            startTime = None
        else:
            startTime = float(token)
        
        offset = None
        endTime = None
        minimumLogLevel = context.get(MINIMUM_LOG_LEVEL_PARAM)
        includeIncomplete = context.get(INCLUDE_INCOMPLETE_PARAM, False)
        includeAppLogs = context.get(INCLUDE_APP_LOGS_PARAM, False)
        versionIds = context.get(VERSION_IDS_PARAM)
        if versionIds and not isinstance(versionIds, list):
            versionIds = [versionIds]
        batchSize = context.get(BATCH_SIZE_PARAM, 10)
        
        logging.info('startTime: %s', startTime)
        
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
            raise Exception('raised to ensure more logs are fetched')
        
        obj[LOG_REQUESTS_PARAM] = logRequests
        
        startTime = logRequests[0].end_time
        return str(startTime)
        
    def execute(self, context, obj):
        """ Handles the logs """
        
        
        logRequests = obj.get(LOG_REQUESTS_PARAM)
        if logRequests:
            logging.info('fetched %d LogRequests' % len(logRequests))
        
        

