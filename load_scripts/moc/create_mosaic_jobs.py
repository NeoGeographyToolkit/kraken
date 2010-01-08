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
from ngt.dispatch.commands.jobcommands import MipMapCommand

ROOTPATH='/big/assets/moc/'
PLATEFILE = 'pf://index/moc_v1.plate'
transaction_id_sequence = Sequence('seq_transaction_id')


def generate_mipmap_jobs(jobset):
    assets = Asset.objects.filter(class_label='scaled image int8')
    for asset in Tracker(iter=assets, target=assets.count(), progress=True):
        job = Job()
        job.transaction_id = transaction_id_sequence.nextval()
        job.command = 'mipmap'
        job.arguments = json.dumps(MipMapCommand.build_arguments(job, platefile=PLATEFILE, file_path=asset.file_path))
        job.footprint = asset.footprint
        job.jobset = jobset
        job.save()
        job.assets.add(asset)
        yield job
        
if __name__ == '__main__':
    jobset = JobSet()
    jobset.name = "Production MipMap"
    jobset.command = "mipmap"
    jobset.save()
    for job in generate_mipmap_jobs(jobset):
        pass
        
    
