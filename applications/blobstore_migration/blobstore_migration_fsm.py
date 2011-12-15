""" blobstore migration 

This system consists of 2 state machines, one that runs on the source (Master-Slave) system
and one that run on the target (HRD) system.

"SendBlobKeysToTargetApp" runs on the source system and is responsible for:

1. continuing over blobstore.BlobInfo entities
2. starting up a target "PullBlobDataFromSourceApp" machine instance on the target system

"PullBlobDataFromSourceApp" runs on the target system and is responsible for:

1. calling http://source//blobinfo_migration/pull_endpoint/ to fetch source blob data
2. saving the blob data into a target blob
3. storing a mapping of sourceBlobKey->targetBlobKey in a BlobKeyMap instance

Idempotency is achieved by keying BlobKeyMap instance off the sourceBlobKeys, so even if multiple
copies are pulled across into the target system, in the end there is a single mapping of source->target.
This has the effect of favoring orphaned blobs on the target system over missing blobs in the target
system.

TODO: User code to resolve source blobs (via source email links) to the target blobs.
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
SOURCE_HOST_PARAM = 'source_host'
TARGET_HOST_PARAM = 'target_host'
SOURCE_BLOB_KEY_PARAM = 'source_blob_key'
SOURCE_BLOB_FILENAME_PARAM = 'source_blob_filename'
SOURCE_BLOB_CONTENT_TYPE_PARAM = 'source_blob_content_type'

SEND_MACHINE_NAME = 'SendBlobKeysToTargetApp'
PULL_MACHINE_NAME = 'PullBlobDataFromSourceApp'

# changing BASE_PATH is allowed, but be sure to also change app.yaml
BASE_PATH = 'blobstore_migration'
START_PULL_URL = 'http://%s/' + BASE_PATH + '/start_pull/'
PULL_ENDPOINT_URL = 'http://%s/' + BASE_PATH + '/pull_endpoint/'

class BlobKeyMap( db.Model ):
    """ Maps Master/Slave blobs to High-Replication blobs """
    sourceBlobKey = db.StringProperty(required=True, indexed=False) # already indexed in key_name
    targetBlobKey = db.StringProperty(required=True)
    
class Send( DatastoreContinuationFSMAction ):
    """ Sends info about source blobs to the target HRD application """
    
    def getQuery(self, context, obj):
        """ For all blobs """
        return blobstore.BlobInfo.all()
    
    def execute(self, context, obj):
        """ Simply POSTs info about the source blob to the target application """
        
        if obj.get(CONTINUATION_RESULT_KEY):
            
            # get the information about the hosts and contruct a url to trigger pull on target app
            sourceHost = context[SOURCE_HOST_PARAM]
            targetHost = context[TARGET_HOST_PARAM]
            startPullUrl = START_PULL_URL % targetHost
            sourceBlobInfo = obj[CONTINUATION_RESULT_KEY]
            taskName = context.get(TASK_NAME_PARAM, DEFAULT_TASK_NAME)
            
            # trigger a pull state machine on the target app
            response = urlfetch.fetch(startPullUrl, 
                                      payload=urllib.urlencode({SOURCE_BLOB_KEY_PARAM: str(sourceBlobInfo.key()),
                                                                SOURCE_BLOB_FILENAME_PARAM: sourceBlobInfo.filename,
                                                                SOURCE_BLOB_CONTENT_TYPE_PARAM: sourceBlobInfo.content_type,
                                                                SOURCE_HOST_PARAM: sourceHost,
                                                                TARGET_HOST_PARAM: targetHost,
                                                                TASK_NAME_PARAM: taskName}),
                                      method=urlfetch.POST,
                                      headers={'Content-Type': 'application/x-www-form-urlencoded'})
            assert response.status_code == 200
            
    @staticmethod
    def getSourceBlobData(sourceBlobKey):
        """ Sends blob data """
        sourceBlobInfo = blobstore.BlobInfo.get(sourceBlobKey)
        blobReader = blobstore.BlobReader(sourceBlobInfo)
        data = blobReader.read() # raises BlobNotFoundError if missing
        return data
    
class Pull( FSMAction ):
    """ Pulls info about source blobs into the target HRD application """
    
    def execute(self, context, obj):
        """ Fetch the source blob data and writes it to the target application """
        
        # fetch the details about the blob
        sourceBlobKey = context[SOURCE_BLOB_KEY_PARAM]
        blobKeyMap = BlobKeyMap.get_by_key_name(sourceBlobKey)
        
        if not blobKeyMap:
            
            sourceBlobContentType = context[SOURCE_BLOB_CONTENT_TYPE_PARAM]
            sourceBlobFilename = context[SOURCE_BLOB_FILENAME_PARAM]
            
            # construct the url in the source app to fetch the blob data
            sourceHost = context[SOURCE_HOST_PARAM]
            pullEndpointUrl = PULL_ENDPOINT_URL % sourceHost + '?' + SOURCE_BLOB_KEY_PARAM + '=' + sourceBlobKey
            
            # fetch the data and assert that the response is valid
            response = urlfetch.fetch(pullEndpointUrl, method=urlfetch.GET)
            assert response.status_code == 200
            
            # now write the blob data back down into the target application
            fileName = files.blobstore.create(mime_type=sourceBlobContentType, 
                                              _blobinfo_uploaded_filename=sourceBlobFilename)
            f = files.open(fileName, 'a')
            f.write(response.content)
            f.close()
            files.finalize(fileName)
            
            # create a mapping between the source blob and the target blob
            targetBlobKey = files.blobstore.get_blob_key(fileName)
            blobKeyMap = BlobKeyMap(key_name=sourceBlobKey, sourceBlobKey=sourceBlobKey, targetBlobKey=str(targetBlobKey))
            blobKeyMap.put()
            
        # perform some optional user data-model migration
        self.migrate(blobKeyMap.sourceBlobKey, blobKeyMap.targetBlobKey)
        
    def migrate(self, sourceBlobKey, targetBlobKey):
        """ Contains user code to migrate data 
        
        @param sourceBlobKey: a string blob key referenceing the source application
        @param targetBlobKey: a string blob key referenceing the target application
        """
        pass
