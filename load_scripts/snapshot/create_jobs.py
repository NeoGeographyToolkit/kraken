from ngt.utils.tracker import Tracker
from django.db import transaction
from ngt.jobs.models import Job,JobSet
from ngt.dispatch.commands.jobcommands import MipMapCommand, hirise2plateCommand, StartSnapshot, EndSnapshot

def _build_snapshot_start_end(transaction_range, jobs_for_dependency, snapshot_jobset, last_endjob, platefile):
    transaction_id = transaction_range[1] + 1
    print "Creating snapshot jobs for transaction range %d --> %d" % transaction_range
    # create start and end jobs
    startjob = Job(
        transaction_id = transaction_id,
        command = 'start_snapshot',
        jobset = snapshot_jobset
    )
    startjob.arguments = StartSnapshot.build_arguments(
        startjob, 
        transaction_range = transaction_range,
        platefile = platefile
    )
    
    endjob = Job(
        transaction_id = transaction_id,
        command = 'end_snapshot',
        jobset = snapshot_jobset
    )
    endjob.arguments = EndSnapshot.build_arguments(endjob, platefile=platefile)
    startjob.save()
    endjob.save()
    # add dependencies
    print "Adding dependencies."
    endjob.dependencies.add(startjob)
    for j in Tracker(iter=jobs_for_dependency, progress=True):
        startjob.dependencies.add(j)
    if last_endjob: # initially not set...
        startjob.dependencies.add(last_endjob)
    return startjob, endjob
    
    

@transaction.commit_on_success   
def create_snapshot_jobs(mmjobset=None, interval=256, platefile=None):
    if not platefile:
        raise Exception("'platefile' argument required.")
    if not mmjobset:
        mmjobset = JobSet.objects.filter(name__contains="MipMap").latest('pk')
    snapshot_jobset = JobSet()
    snapshot_jobset.name = "mosaic snapshots (js%d)" % mmjobset.id
    snapshot_jobset.command = "snapshot"
    snapshot_jobset.save()

    i = 0
    transaction_range_start = None
    jobs_for_dependency = []
    endjob = None
    for mmjob in mmjobset.jobs.all().order_by('transaction_id'):
        i += 1
        jobs_for_dependency.append(mmjob)
        if not transaction_range_start:
            transaction_range_start = mmjob.transaction_id        
        if i % interval == 0:
            transaction_range = (transaction_range_start, mmjob.transaction_id)
            startjob, endjob = _build_snapshot_start_end(transaction_range, jobs_for_dependency, snapshot_jobset, endjob, platefile)
            #clear transaction range and jobs for dependency list
            transaction_range_start = mmjob.transaction_id + 1  # Set the start of the next snapshot
            jobs_for_dependency = []
    else: # after the last iteration, start a snapshot with whatever's left.
        if jobs_for_dependency:
            transaction_range = (transaction_range_start, mmjob.transaction_id)
            _build_snapshot_start_end(transaction_range, jobs_for_dependency, snapshot_jobset, endjob, platefile)
    print "Setting priority to 1 and activating."
    snapshot_jobset.priority = 1
    snapshot_jobset.active = True
    snapshot_jobset.save()
    return snapshot_jobset
