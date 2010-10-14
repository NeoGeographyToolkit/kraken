import sys, os, json
import itertools

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
from ngt.dispatch.commands.jobcommands import MipMapCommand, StartSnapshot, EndSnapshot
from load_scripts.snapshot.create_jobs import create_snapshot_jobs


PLATEFILE = 'pf://wwt10one/index/hrsc_v1.plate'
transaction_id_sequence = Sequence('seq_transaction_id')


def _build_mipmap_jobs(jobset, asset_queryset, count=None):
    if not count:
        count = asset_queryset.count()
    for asset in Tracker(iter=asset_queryset, target=count, progress=True):
        job = Job()
        while True:  # Get the next even transaction ID
            job.transaction_id = transaction_id_sequence.nextval()
            if job.transaction_id % 2 == 0:
                break
        job.command = 'mipmap'
        job.arguments = json.dumps(job.wrapped().build_arguments(platefile=PLATEFILE, file_path=asset.file_path))
        job.footprint = asset.footprint
        job.jobset = jobset
        job.save()
        job.assets.add(asset)
        
@transaction.commit_on_success
def create_mipmap_jobs(n_jobs=None, basemap=True):
    # where n_jobs is the number of jobs to generate.  Default (None) builds jobs for all assets in the queryset.
    transaction_id_sequence.setval(1) # reset the transaction_id sequence
    mola_assets = Asset.objects.filter(class_label='mola basemap')
    hrsc_assets = Asset.objects.filter(class_label='hrsc')[:n_jobs]
    assets = itertools.chain(mola_assets, hrsc_assets)
    jobset = JobSet()
    jobset.name = "HRSC MipMap (%s)" % (n_jobs or 'all')
    jobset.command = "mipmap"
    jobset.priority = 3
    jobset.save()
    _build_mipmap_jobs(jobset, assets, count=mola_assets.count() + hrsc_assets.count())
    return jobset
