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
from ngt.dispatch.commands.jobcommands import MipMapCommand, hirise2plateCommand, StartSnapshot, EndSnapshot
from load_scripts.snapshot.create_jobs import create_snapshot_jobs

PLATEFILE = 'pf://wwt10one/index/hirise_v2.plate'
transaction_id_sequence = Sequence('seq_transaction_id')
        
def _build_jobs(command_class, jobset, asset_queryset):
    for asset in Tracker(iter=asset_queryset, target=asset_queryset.count(), progress=True):
        job = Job()
        while True:  # Get the next even transaction ID
            job.transaction_id = transaction_id_sequence.nextval()
            if job.transaction_id % 2 == 0:
                break
        job.command = command_class.name
        job.arguments = command_class.build_arguments(job, platefile=PLATEFILE, file_path=asset.file_path)
        job.footprint = asset.footprint # TODO: Generate footprints from label metadata.
        job.jobset = jobset
        job.save()
        job.assets.add(asset)
        
@transaction.commit_on_success
def create_mipmap_jobs(n_jobs=None, basemap=False):
    # where n_jobs is the number of jobs to generate.  Default (None) builds jobs for all assets in the queryset.
    transaction_id_sequence.setval(1) # reset the transaction_id sequence
    assets = Asset.objects.filter(class_label='hirise product', md5_check=True)[:n_jobs]
    jobset = JobSet()
    jobset.name = "hirise2plate (%s)" % (n_jobs or 'all')
    jobset.command = "hirise2plate"
    jobset.priority = 3
    jobset.save()
    if basemap:
        _build_jobs(MipMapCommand, jobset, Asset.objects.filter(class_label='color basemap'))
    _build_jobs(hirise2plateCommand, jobset, assets)
    return jobset

