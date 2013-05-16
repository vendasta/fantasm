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

Release Notes:

v1.2.0
- allow the capabilities check to be configured with "enable_capabilties_check" in fantasm.yaml (default True)
- fixed https://code.google.com/p/fantasm/issues/detail?id=8
- fixes https://code.google.com/p/fantasm/issues/detail?id=10

v1.1.1
- very minor bug fix

v1.1.0
- added fantasm.exceptions.HaltMachineError; when raised the machine will be stopped without needing
  to specify "final: True" on the state. Normally if a None event is returned from a "final: False"
  state, Fantasm complains loudly. HaltMachineError provides a way to kill a machine without this
  loud complaint in the logs, though the HaltMachineError allows you to provide a log message and
  a log level to log at (logLevel=None means do not emit a message at all).

v1.0.1
- fixed an issue with context.setQueue()

v1.0.0
- we've been out for a long time, but never had formal release notes. Gotta start somewhere!

"""

__version__ = '1.2.0'

# W0401:  2: Wildcard import fsm
# pylint: disable-msg=W0401
from fantasm.fsm import *
