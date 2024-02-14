""" Test machine for built-in TaskRetryOptions. """

class State1:
    
    def execute(self, context, obj):
        return 'ok'
        
class State2:
    
    def execute(self, context, obj):
        raise Exception()
