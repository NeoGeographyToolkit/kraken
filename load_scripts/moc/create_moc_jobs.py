
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

ROOTPATH='/big/sourcedata/moc'
DESTPATH='/big/assets/MOC/'

def generate_jobs(queryset):
    track = None
    for asset in queryset.get_query_set():
        if not track:
            track = Tracker(target=queryset.count())
        job = Job(command='moc-stage')
        sourcefile = asset.file_path
        dest_name = os.path.splitext(asset.file_name.lower())[0] + '.cub'
        destfile =  os.path.join(DESTPATH, asset.volume.lower(), dest_name)
        if abs(asset.center_latitude) > 85:
            map_projection = 'PolarStereographic'
        else:
            map_projection = 'Sinusoidal'

        job.arguments = json.dumps( ("%s %s --map %s" % (sourcefile, destfile, map_projection) ).split(' '))
        track.next()
        job.save()
        job.assets.add(asset)
        yield job

@transaction.commit_on_success
def populate_jobs(jobset):
    for job in Tracker(iter=generate_jobs(jobset.assets), target=jobset.assets.count(), progress=True):
        jobset.jobs.add(job)

    transaction.commit()
    return jobset.jobs

@transaction.commit_on_success
def create_jobset():
    assets = Asset.objects.filter(class_label='MOC SDP', md5_check=True)
    set = JobSet()
    set.name = 'MOC Staging'
    set.command='moc-stage'
    set.save()
    print "Saved JobSet %d.  Adding assets." % set.id
    for asset in Tracker(iter=assets.iterator(), target=assets.count(), progress=True):         set.assets.add(asset)
    transaction.commit()
    return set
