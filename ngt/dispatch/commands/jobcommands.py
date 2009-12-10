class JobCommand(object):
    
    name = None
    number_of_args = 0
    
    @classmethod
    def check_readiness(klass, job):
        ''' Return True if the system is ready to process this job, False otherwise. '''
        return True
    
    @classmethod    
    def preprocess_job(klass, job):
        return job
    
    @classmethod
    def postprocess_job(klass, job):
        return job
        
class MosaicJobCommand(JobCommand): 
    name = 'mosaic'
    number_of_args = 2
    current_footprints = {}

    @classmethod
    def check_readiness(klass, job):
        footprint = job.assets[0].footprint.prepared
        for other_footprint in klass.current_footprints:
            if other_footprint.touches(footprint):
                return False
        else:
            return True

    @classmethod
    def preprocess_job(klass, job):
        klass.current_footprints[job.uuid] = job.assets[0].footprint.prepared
        return job

    @classmethod
    def postprocess_job(klass, job):
        del klass.current_footprints[job.uuid]
        return job  