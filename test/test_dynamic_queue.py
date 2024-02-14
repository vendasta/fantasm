"""
Testing dynamic queue specificiation (context.setQueue())
"""
import os
import logging

from .fantasm.action import FSMAction

OK_EVENT = 'ok'

def log_environ():
    for key, value in os.environ.items():
        if not key.startswith('HTTP_X_APPENGINE_'):
            continue
        logging.info('%s: %s', key, value)

class OnDefaultQueue(FSMAction):
    def execute(self, context, obj):
        log_environ()
        context.setQueue('alternate-queue')
        return OK_EVENT

class OnDynamicQueue(FSMAction):
    def execute(self, context, obj):
        log_environ()
        context.setQueue('default')
        return OK_EVENT

class BackOnDefaultQueue(FSMAction):
    def execute(self, context, obj):
        log_environ()
        return None
