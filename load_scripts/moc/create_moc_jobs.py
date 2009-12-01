
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

ROOTPATH='/big/assets/mocsource/'
DESTPATH='/big/assets/moc/'

def generate_mocproc_jobs(jobset):
    track = None
    queryset = jobset.assets
    for asset in queryset.get_query_set():
        if not track:
            track = Tracker(target=queryset.count())
        job = Job(command=jobset.command)
        sourcefile = asset.file_path
        basename = '/'.join(asset.file_path.split('/')[-2:])
        dest_name = os.path.splitext(basename)[0] + '.cub'
        destfile =  os.path.join(DESTPATH, asset.volume.lower(), dest_name)
        try:
            centerlat = asset.footprint.centroid.y
        except AttributeError:  # indicates an incomplete or funnky geometry
            centerlat = asset.footprint.convex_hull.centroid.y
        if abs(centerlat) > 85:
            map_projection = 'PolarStereographic'
        else:
            map_projection = 'Sinusoidal'

        job.arguments = json.dumps( ("%s %s --map %s" % (sourcefile, destfile, map_projection) ).split(' '))
        track.next()
        job.jobset = jobset
        job.save()
        job.assets.add(asset)
        yield job

@transaction.commit_on_success
def populate_mocproc_jobs(jobset):
    for job in Tracker(iter=generate_jobs(jobset), target=jobset.assets.count(), progress=True):
        jobset.jobs.add(job)

    transaction.commit()
    return jobset.jobs

@transaction.commit_on_success
def create__mocproc_jobset():
    assets = Asset.objects.filter(class_label='MOC SDP', md5_check=True)
    set = JobSet()
    set.name = 'MOC Staging'
    set.command='moc-stage'
    set.save()
    print "Saved JobSet %d.  Adding assets." % set.id
    for asset in Tracker(iter=assets.iterator(), target=assets.count(), progress=True):
        set.assets.add(asset)
    transaction.commit()
    return set
    
    
@transaction.commit_on_successs
def create_scale2int8_jobset():
    assets = Asset.objects.filter(class_label='mocprocd image')
    set = JobSet()
    set.name = "int8 scaling"
    set.command = 'scale2int8'
    set.output_asset_label = "scaled image int8"
    for asset in Tracker(iter=assets.iterator(), target=assets.count(), progress=True):
        set.assets.add(asset)
    return set