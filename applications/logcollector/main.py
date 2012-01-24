""" log collector CGI script
"""
import os

from google.appengine.ext import webapp
from google.appengine.ext.webapp import util

from logcollector_fsm import VERSION_IDS_PARAM
from logcollector_fsm import SENDER_EMAIL_PARAM
from logcollector_fsm import TO_EMAIL_PARAM

import fantasm

LOG_COLLECTOR_MACHINE_NAME = 'LogCollector'

CONTENT = """
<html>
<head>
<title>Start LogCollector</title>
</head>
<body>
<form method="POST">
Version: <input name="version-ids" value="" size="25"/><br/>
Minimum Log Level: <select name="minimum-log-level">
  <option value="4">CRITICAL</option>
  <option value="3">ERROR</option>
  <option value="2">WARNING</option>
  <option value="1">INFO</option>
  <option value="0">DEBUG</option>
</select><br/>
Sender: <input name="sender" value="" size="25"/><br/>
To: <input name="to" value="" size="25"/><br/>
<input type="submit"/>
</form>
</body>
</html>
"""

CURRENT_VERSION_ID = os.environ['CURRENT_VERSION_ID'].split('.')[0]
            
class StartLogCollectorHandler(webapp.RequestHandler):
    """ Starts the LogCollector machine """
    
    def get(self):
        """ see RequestHandler.get() """
        self.response.out.write(CONTENT)
    
    def post(self):
        """ see RequestHandler.post() """
        assert VERSION_IDS_PARAM in self.request.POST.keys(), "Require deployed version to monitor."
        assert self.request.POST[VERSION_IDS_PARAM] != CURRENT_VERSION_ID, "Cannot monitor own version."
        assert SENDER_EMAIL_PARAM in self.request.POST.keys(), "Require admin sender email address."
        assert TO_EMAIL_PARAM in self.request.POST.keys(), "Require email address."
        fantasm.startStateMachine(LOG_COLLECTOR_MACHINE_NAME, [self.request.POST])
        self.response.out.write("LogCollector started on version '%s'." % self.request.POST[VERSION_IDS_PARAM])
        
def createApplication():
    """ Returns a WSGI Application """
    return webapp.WSGIApplication([
        (r"^/logcollector/", StartLogCollectorHandler),
    ],
    debug=True)

APP = createApplication()

def main():
    """ Main entry point. """
    util.run_wsgi_app(APP)

if __name__ == "__main__":
    main()
