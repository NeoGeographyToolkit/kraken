from ngt.messaging.queue import messagebus
from threading import Thread

class StatusDaemon(object):
    
    '''A singleton Instance that listens for job status updates.'''
    
    class __impl(object):
    
        def _set_status(jobid, status):
            "Set status of job %s to %s." % (jobid, status)
            pass
        
        def _process_msg(msg):
            """unpack message and update status"""
            pass
            
        
        def __init__(self):
            print "Status Daemon is Launching."
            """connect qlistener to messagebus"""
            messagebus.setup_direct_queue('status')
            
            def qlistener():
                messsagebus._chan.basic_consume(queue='status', callback=_process_msg)
                while True:
                    messagebus._chan.wait()

            pass 

    __instance = None
    def __init__(self):
        """ Create a singleton instance of the status daemon"""

        # Check to see if we already have an instance
        if (StatusDaemon.__instance is None):
            # Create and remember instance
            StatusDaemon.__instance = StatusDaemon.__impl();
        
        # Store instance reference as the only member in the handle
        self.__dict__['_StatusDaemon__instance'] = StatusDaemon.__instance


    def __getattr__(self, attr):
        """ Delegate access to implementation """
        return getattr(self.__instance, attr)

    def __setattr__(self, attr, value):
        """ Delegate access to implementation """
        return setattr(self.__instance, attr, value)

if __name__ == "__main__":
    StatusDaemon()
