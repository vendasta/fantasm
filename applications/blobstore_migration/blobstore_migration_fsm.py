""" blobstore migration 

This system consists of 2 state machines, one that runs on the old (Master-Slave) system
and one that run on the new (HRD) system.

"SendBlobKeysToNewApp" runs on the old system and is responsible for:

1. continuing over blobstore.BlobInfo entities
2. starting up a new "PullBlobDataFromOldApp" machine instance on the new system

"PullBlobDataFromOldApp" runs on the new system and is responsible for:

1. calling http://old//blobinfo_migration/pull_endpoint/ to fetch old blob data
2. saving the blob data into a new blob
3. storing a mapping of oldBlobKey->newBlobKey in a BlobKeyMap instance

Idempotency is achieved by keying BlobKeyMap instance off the oldBlobKeys, so even if multiple
copies are pulled across into the new system, in the end there is a single mapping of old->new.
This has the effect of favoring orphaned blobs on the new system over missing blobs in the new
system.

TODO: User code to resolve old blobs (via old email links) to the new blobs.
"""
import urllib

from fantasm.action import DatastoreContinuationFSMAction
from fantasm.action import FSMAction
from fantasm.constants import CONTINUATION_RESULT_KEY

from google.appengine.api import files
from google.appengine.ext.blobstore import blobstore
from google.appengine.api import urlfetch
from google.appengine.ext import db

DEFAULT_TASK_NAME = 'blobstore-migration'

TASK_NAME_PARAM = 'task_name'
OLD_HOST_PARAM = 'old_host'
NEW_HOST_PARAM = 'new_host'
OLD_BLOB_KEY_PARAM = 'old_blob_key'
OLD_BLOB_FILENAME_PARAM = 'old_blob_filename'
OLD_BLOB_CONTENT_TYPE_PARAM = 'old_blob_content_type'

SEND_MACHINE_NAME = 'SendBlobKeysToNewApp'
PULL_MACHINE_NAME = 'PullBlobDataFromOldApp'

# changing BASE_PATH is allowed, but be sure to also change app.yaml
BASE_PATH = 'blobstore_migration'
START_PULL_URL = 'http://%s/' + BASE_PATH + '/start_pull/'
PULL_ENDPOINT_URL = 'http://%s/' + BASE_PATH + '/pull_endpoint/'

class BlobKeyMap( db.Model ):
    """ Maps Master/Slave blobs to High-Replication blobs """
    oldBlobKey = db.StringProperty(required=True, indexed=False) # already indexed in key_name
    newBlobKey = db.StringProperty(required=True)
    
class Send( DatastoreContinuationFSMAction ):
    """ Sends info about old blobs to the new HRD application """
    
    def getQuery(self, context, obj):
        """ For all blobs """
        return blobstore.BlobInfo.all()
    
    def execute(self, context, obj):
        """ Simply POSTs info about the old blob to the new application """
        
        if obj.get(CONTINUATION_RESULT_KEY):
            
            # get the information about the hosts and contruct a url to trigger pull on new app
            oldHost = context[OLD_HOST_PARAM]
            newHost = context[NEW_HOST_PARAM]
            startPullUrl = START_PULL_URL % newHost
            oldBlobInfo = obj[CONTINUATION_RESULT_KEY]
            taskName = context.get(TASK_NAME_PARAM, DEFAULT_TASK_NAME)
            
            # trigger a pull state machine on the new app
            response = urlfetch.fetch(startPullUrl, 
                                      payload=urllib.urlencode({OLD_BLOB_KEY_PARAM: str(oldBlobInfo.key()),
                                                                OLD_BLOB_FILENAME_PARAM: oldBlobInfo.filename,
                                                                OLD_BLOB_CONTENT_TYPE_PARAM: oldBlobInfo.content_type,
                                                                OLD_HOST_PARAM: oldHost,
                                                                NEW_HOST_PARAM: newHost,
                                                                TASK_NAME_PARAM: taskName}),
                                      method=urlfetch.POST,
                                      headers={'Content-Type': 'application/x-www-form-urlencoded'})
            assert response.status_code == 200
            
    @staticmethod
    def getOldBlobData(oldBlobKey):
        """ Sends blob data """
        oldBlobInfo = blobstore.BlobInfo.get(oldBlobKey)
        blobReader = blobstore.BlobReader(oldBlobInfo)
        data = blobReader.read() # raises BlobNotFoundError if missing
        return data
    
class Pull( FSMAction ):
    """ Pulls info about old blobs into the new HRD application """
    
    def execute(self, context, obj):
        """ Fetch the old blob data and writes it to the new application """
        
        # fetch the details about the blob
        oldBlobKey = context[OLD_BLOB_KEY_PARAM]
        blobKeyMap = BlobKeyMap.get_by_key_name(oldBlobKey)
        
        if not blobKeyMap:
            
            oldBlobContentType = context[OLD_BLOB_CONTENT_TYPE_PARAM]
            oldBlobFilename = context[OLD_BLOB_FILENAME_PARAM]
            
            # construct the url in the old app to fetch the blob data
            oldHost = context[OLD_HOST_PARAM]
            pullEndpointUrl = PULL_ENDPOINT_URL % oldHost + '?' + OLD_BLOB_KEY_PARAM + '=' + oldBlobKey
            
            # fetch the data and assert that the response is valid
            response = urlfetch.fetch(pullEndpointUrl, method=urlfetch.GET)
            assert response.status_code == 200
            
            # now write the blob data back down into the new application
            fileName = files.blobstore.create(mime_type=oldBlobContentType, 
                                              _blobinfo_uploaded_filename=oldBlobFilename)
            f = files.open(fileName, 'a')
            f.write(response.content)
            f.close()
            files.finalize(fileName)
            
            # create a mapping between the old blob and the new blob
            newBlobKey = files.blobstore.get_blob_key(fileName)
            blobKeyMap = BlobKeyMap(key_name=oldBlobKey, oldBlobKey=oldBlobKey, newBlobKey=str(newBlobKey))
            blobKeyMap.put()
            
        # perform some optional user data-model migration
        self.migrate(blobKeyMap.oldBlobKey, blobKeyMap.newBlobKey)
        
    def migrate(self, oldBlobKey, newBlobKey):
        """ Contains user code to migrate data 
        
        @param oldBlobKey: a string blob key referenceing the old application
        @param newBlobKey: a string blob key referenceing the new application
        """
        pass
