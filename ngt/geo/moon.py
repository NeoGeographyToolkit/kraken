import os
import util
#from ngt.jobs.models import RemoteJob

class Moon(object):

    def altitude(self, lat, lon):
        #job = RemoteJob("altitude moon " + str(lon) + " " + str(lat))
#        job.wait()
        return 10.0 #float(job.result())

    def radius(self, lat, lon):
        return 0
#        job = RemoteJob("RADIUS", "MOON " + str(lon) + " " + str(lat))
#        job.wait()
#        return float(job.result());
