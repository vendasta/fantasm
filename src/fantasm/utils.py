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
from google.appengine.api.taskqueue.taskqueue import Queue

class NoOpQueue( Queue ):
    """ A Queue instance that does not Queue """
    
    def add(self, task, transactional=False):
        """ see taskqueue.Queue.add """
        pass
       
def knuthHash(number):
    """A decent hash function for integers."""
    return (number * 2654435761) % 2**32

def boolConverter(boolStr):
    """ A converter that maps some common bool string to True """
    return {'1': True, 'True': True, 'true': True}.get(boolStr, False)
