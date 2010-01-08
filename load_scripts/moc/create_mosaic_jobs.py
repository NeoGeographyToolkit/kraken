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
from ngt.dispatch.commands.jobcommands import MipMapJob

ROOTPATH='/big/assets/moc/'
PLATEFILE = 'pf://index/moc_v1.plate'
transaction_id_sequence = Sequence('seq_transaction_id')


def generate_image2plate_jobs():
    assets = Asset.objects.filter(class_label='scaled image int8')
    for asset in assets:
        job = Job()
        job.transaction_id = transaction_id_sequence.nextval()
        job.command = 'mipmap'
        job.arguments = json.dumps(MipMapJob(job, platefile=PLATEFILE))
        job.footprint = asset.footprint
        #job.save()
        #job.assets.add(asset)
        print job.command + ' ' + json.loads(job.arguments)
        
if __name__ == '__main__':
    generate_image2plate_jobs()