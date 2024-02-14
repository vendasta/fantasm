""" blobstore migration CGI script
"""
import os

from google.appengine.ext import webapp
from google.appengine.ext.webapp import util
from google.appengine.ext.blobstore import blobstore
from google.appengine.api.taskqueue import TaskAlreadyExistsError
from google.appengine.api.taskqueue import TombstonedTaskError

from blobstore_migration_fsm import Send
from blobstore_migration_fsm import SOURCE_BLOB_KEY_PARAM
from blobstore_migration_fsm import SOURCE_HOST_PARAM
from blobstore_migration_fsm import TARGET_HOST_PARAM
from blobstore_migration_fsm import TASK_NAME_PARAM
from blobstore_migration_fsm import DEFAULT_TASK_NAME
from blobstore_migration_fsm import SEND_MACHINE_NAME
from blobstore_migration_fsm import PULL_MACHINE_NAME

import fantasm

class PullEndpointHandler(webapp.RequestHandler):
    """ Listening on the Master-Slave application """
    def get(self):
        """ see RequestHandler.get() """
        sourceBlobKey = self.request.GET[SOURCE_BLOB_KEY_PARAM]
        try:
            sourceBlobData = Send.getSourceBlobData(sourceBlobKey)
            self.response.out.write(sourceBlobData)
        except blobstore.BlobNotFoundError:
            self.error(404)
            
CONTENT = """
<html>
<head>
<title>Blobstore Data Migration</title>
</head>
<body>
<form method="POST">
Source: http://<input name="source_host" value="%(source_host)s" size="50"/>%(path)s<br/>
Target: http://<input name="target_host" value="%(target_host)s" size="50"/>%(path)s<br/>
Task Name: <input name="task_name" value="%(task_name)s" size="25"/><br/>
<input type="submit"/>
</form>
</body>
</html>
"""

PATH = 'path'
            
class StartMigrationHandler(webapp.RequestHandler):
    """ Listening on the Master-Slave application """
    
    def get(self):
        """ see RequestHandler.get() """
        version = os.environ['CURRENT_VERSION_ID'].split('.')[0]
        sourceHost = '%s.FIXME_SOURCE.appspot.com' % (version,)
        targetHost = '%s.FIXME_TARGET.appspot.com' % (version,)
        content = CONTENT % {SOURCE_HOST_PARAM: sourceHost,
                             TARGET_HOST_PARAM: targetHost,
                             TASK_NAME_PARAM: DEFAULT_TASK_NAME,
                             PATH: self.request.path_info}
        self.response.out.write(content)
    
    def post(self):
        """ see RequestHandler.post() """
        assert SOURCE_HOST_PARAM in list(self.request.POST.keys())
        assert TARGET_HOST_PARAM in list(self.request.POST.keys())
        
        # using one-time taskName ensure we don't start the migration twice
        taskName = self.request.POST.get(TASK_NAME_PARAM, DEFAULT_TASK_NAME)
        try:
            fantasm.startStateMachine(SEND_MACHINE_NAME, [self.request.POST], taskName=taskName, raiseIfTaskExists=True)
            self.response.out.write("Success!!!")
        except (TaskAlreadyExistsError, TombstonedTaskError):
            self.error(403)
            self.response.out.write("Failed!!! (blobstore migration previously started)")
        
class StartPullHandler(webapp.RequestHandler):
    """ Listening on the High-Replication application """
    def post(self):
        """ see RequestHandler.get() """
        # using the source blob key in the task name ensures we don't pull the same blob multiple times
        taskName = self.request.POST.get(TASK_NAME_PARAM, DEFAULT_TASK_NAME)
        taskName = taskName + '-' + self.request.POST[SOURCE_BLOB_KEY_PARAM].replace('=', '-') # '=' on dev_appserver
        fantasm.startStateMachine(PULL_MACHINE_NAME, [self.request.POST], taskName=taskName, raiseIfTaskExists=False)
        
def createApplication():
    """ Returns a WSGI Application """
    return webapp.WSGIApplication([
        (r"^/blobstore_migration/start_migration/", StartMigrationHandler),
        (r"^/blobstore_migration/start_pull/",      StartPullHandler),
        (r"^/blobstore_migration/pull_endpoint/",   PullEndpointHandler),
    ],
    debug=True)

APP = createApplication()

def main():
    """ Main entry point. """
    util.run_wsgi_app(APP)

if __name__ == "__main__":
    main()
