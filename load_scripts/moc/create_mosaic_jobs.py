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


def generate_mipmap_jobs(jobset):
    assets = Asset.objects.filter(class_label='scaled image int8')[:1000]
    for asset in Tracker(iter=assets, target=assets.count(), progress=True):
        job = Job()
        job.transaction_id = transaction_id_sequence.nextval()
        job.command = 'mipmap'
        job.arguments = json.dumps(MipMapCommand.build_arguments(job, platefile=PLATEFILE, file_path=asset.file_path))
        job.footprint = asset.footprint
        job.jobset = jobset
        job.save()
        job.assets.add(asset)
        #yield job
        
@transaction.commit_on_success
def create_mipmap_jobs():
    jobset = JobSet()
    jobset.name = "Debug MipMap"
    jobset.command = "mipmap"
    jobset.save()
    generate_mipmap_jobs(jobset)
        

def build_snapshot_start_end(transaction_range, jobs_for_dependency):
    transaction_id = transaction_id_sequence.nextval()
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
            build_snapshot_start_end(transaction_range, jobs_for_dependency)
            #clear transaction range and jobs for dependency list
            transaction_range_start = None
            jobs_for_dependency = []
    else: # after the last iteration, start a snapshot with whatever's left.
        transaction_range = (transaction_range_start, mmjob.transaction_id)
        build_snapshot_start_end(transaction_range, jobs_for_dependency)
