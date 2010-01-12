import sys, os, json

sys.path.insert(0, '../..')
os.environ['DJANGO_SETTINGS_MODULE'] = 'ngt.settings'
from django.core.management import setup_environ
from django.db import transaction
from ngt import settings
setup_environ(settings)

from ngt.jobs.models import JobSet, Job
from ngt.assets.models import Asset
from ngt.utils.tracker import Tracker
from ngt.django_extras.db.sequence import Sequence
from ngt.dispatch.commands.jobcommands import MipMapCommand, StartSnapshot

ROOTPATH='/big/assets/moc/'
PLATEFILE = 'pf://wwt10one/index/moc_v1.plate'
transaction_id_sequence = Sequence('seq_transaction_id')


def _build_mipmap_jobs(jobset, asset_queryset):
    for asset in Tracker(iter=asset_queryset, target=asset_queryset.count(), progress=True):
        job = Job()
        while True:  # Get the next even transaction ID
            job.transaction_id = transaction_id_sequence.nextval()
            if job.transaction_id % 2 == 0:
                break
        job.command = 'mipmap'
        job.arguments = json.dumps(MipMapCommand.build_arguments(job, platefile=PLATEFILE, file_path=asset.file_path))
        job.footprint = asset.footprint
        job.jobset = jobset
        job.save()
        job.assets.add(asset)
        
@transaction.commit_on_success
def create_mipmap_jobs(n_jobs=None):
    # where n_jobs is the number of jobs to generate.  Default (None) builds jobs for all assets in the queryset.
    transaction_id_sequence.setval(1) # reset the transaction_id sequence
    assets = Asset.objects.filter(class_label='scaled image int8')[:n_jobs]
    jobset = JobSet()
    jobset.name = "Debug MipMap"
    jobset.command = "mipmap"
    jobset.save()
    _build_mipmap_jobs(jobset, assets)
        

def _build_snapshot_start_end(transaction_range, jobs_for_dependency):
    # transaction_id = transaction_id_sequence.nextval() # TODO: this is now wrong.  Should be user-specified, and the range can be inferred
    print "Creating snapshot jobs for transaction range %d --> %d" % transaction_range
    # create start and end jobs
    startjob = Job(
        transaction_id = transaction_id,
        command = 'start_snapshot',
        jobset = snapshot_jobset
    )
    startjob.arguments = json.dumps(
        StartSnapshot.build_arguments(
            startjob, 
            transaction_range = transaction_range,
            platefile = PLATEFILE
        )
    )
    
    endjob = Job(
        transaction_id = transaction_id,
        command = 'end_snapshot',
        jobset = snapshot_jobset
    )
    endjob.arguments = json.dumps(EndSnapshot.build_arguments(endjob))
    startjob.save()
    endjob.save()
    # add dependencies
    print "Adding dependencies."
    endjob.dependencies.add(startjob)
    for j in Tracker(iter=jobs_for_dependency, progress=True):
        startjob.dependencies.add(j)
    
    

@transaction.commit_on_success   
def create_snapshot_jobs():
    snapshot_jobset = JobSet()
    jobset.name = "mosaic snapshots"
    jobset.command = "snapshot"

    mmjobset = JobSet.objects.filter(name="Production MipMap").latest('pk')
    i = 0
    transaction_range_start = None
    jobs_for_dependency = []
    for mmjob in mmjobset.jobs.all():
        i += 1
        jobs_for_dependency.append(mmjob)
        if not transaction_range_start:
            transaction_range_start = mmjob.transaction_id        
        if i % 256 == 0:
            transaction_range = (transaction_range_start, mmjob.transaction_id)
            _build_snapshot_start_end(transaction_range, jobs_for_dependency)
            #clear transaction range and jobs for dependency list
            transaction_range_start = None
            jobs_for_dependency = []
    else: # after the last iteration, start a snapshot with whatever's left.
        transaction_range = (transaction_range_start, mmjob.transaction_id)
        _build_snapshot_start_end(transaction_range, jobs_for_dependency)
