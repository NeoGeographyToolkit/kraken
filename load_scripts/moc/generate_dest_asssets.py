import sys, os, json
from copy import copy

sys.path.insert(0, '../..')
os.environ['DJANGO_SETTINGS_MODULE'] = 'ngt.settings'
from django.core.management import setup_environ
from django.db import transaction
from ngt import settings
setup_environ(settings)

from ngt.jobs.models import JobSet, Job
from ngt.assets.models import Asset, DATA_ROOT
from ngt.utils.tracker import Tracker

ROOTPATH='/big/assets/mocsource/'
DESTPATH='/big/assets/moc/'

def gen_jobs(js_id):
    js = JobSet.objects.get(pk=js_id)
    for job in js.jobs.filter(status='complete'):
        yield job
        
@transaction.commit_manually
def main():
    JOBSET = 3
    for job in Tracker(iter=gen_jobs()):
        asset_o = job.assets.all()[0]
        asset_n = copy(asset_o)
        asset_n.id = None
        asset_n.is_original = False
        asset_n.relative_file_path = job.arguments[1].replace(DATA_ROOT,'')
        asset_n.class_label = "mocproc'd moc image"
        asset_n.save()
        job.assets.add(asset_n)
        asset_n.parents.add(asset_o)
        
        
if __name__ == '__main__':
    main()
        