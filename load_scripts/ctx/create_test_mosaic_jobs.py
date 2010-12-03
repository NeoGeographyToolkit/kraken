#!/usr/bin/env python
import sys, os, json
import csv
import optparse

sys.path.insert(0, '../..')
os.environ['DJANGO_SETTINGS_MODULE'] = 'ngt.settings'
from django.core.management import setup_environ
from django.db import transaction
from ngt import settings
setup_environ(settings)

from ngt.protocols import dotdict
from ngt.jobs.models import JobSet, Job
from ngt.utils.tracker import Tracker
#from ngt.django_extras.db.sequence import Sequence
from ngt.dispatch.commands.jobcommands import ctx2plateCommand, StartSnapshot, EndSnapshot
from load_scripts.snapshot.create_jobs import create_snapshot_jobs


METADATA_DIR = '/big/sourcedata/mars/ctx/meta'
DEFAULT_PLATEFILE = 'pf://wwt10one/index/test_ctx_default.plate'
#transaction_id_sequence = Sequence('seq_transaction_id')

def gen_transaction_ids():
    i = 0
    while True:
        i += 2
        yield i

def _build_mipmap_jobs(jobset, urls, platefile, downsample=None, n_jobs=None):
    transaction_ids = gen_transaction_ids()
    i = 0
    for url in Tracker(iter=urls, target=27859, progress=True):
        job = Job()
        job.transaction_id = transaction_ids.next()
        job.command = 'ctx2plate'
        job.arguments = job.wrapped().build_arguments(url=url, platefile=platefile, transaction_id=job.transaction_id, downsample=downsample)
        job.jobset = jobset
        job.save()
        i += 1
        if n_jobs and i >= n_jobs: break
    print "Created %d jobs." % i

def generate_urls(metadata_dir=METADATA_DIR, baseurl='http://pds-imaging.jpl.nasa.gov/data/mro/mars_reconnaissance_orbiter/ctx/'):
    #indextable = os.path.join(metadata_dir, 'cumindex.tab')
    indextable = os.path.join(metadata_dir, 'ctx_1000.csv')
    indexReader = csv.reader(open(indextable, 'r'))
    for row in indexReader:
        if len(row) < 2: continue # skip blank lines
        volume, filespec = row[0:2]
        volume = volume.lower()
        head, tail = os.path.split(filespec)
        head = head.lower()
        url = os.path.join(baseurl, volume, head, tail)
        yield url
        
@transaction.commit_on_success
def create_mipmap_jobs(n_jobs=None, platefile=DEFAULT_PLATEFILE, name=None, downsample=None):
    # where n_jobs is the number of jobs to generate.  Default (None) builds jobs for all assets in the queryset.
    #transaction_id_sequence.setval(1) # reset the transaction_id sequence
    jobset = JobSet()
    jobset.name = name or "CTX MipMap (%s)" % (n_jobs or 'all')
    jobset.command = "ctx2plate"
    jobset.priority = 3
    jobset.save()
    _build_mipmap_jobs(jobset, generate_urls(), platefile, downsample=downsample, n_jobs=n_jobs)
    return jobset

def main():
    parser = optparse.OptionParser()
    parser.add_option('-p', '--platefile', action='store', dest='platefile', help='Platefile URL to which the images should be written (e.g. pf://wwt10one/index/collectible.plate)')
    parser.add_option('--njobs', action="store", dest="n_jobs", type="int", help="Limit the number of image2plate jobs generated")
    parser.add_option('--no-activate', action='store_false', dest='activate', help='Do not activate the new jobsets after creation.')
    parser.add_option('--name', action='store', dest='jobset_name', help="Override the default name for the image2plate jobset.")
    parser.add_option('--no-snapshots', action='store_false', dest='do_snapshots', help="Don't create a snapshot JobSet.")
    parser.set_defaults(platefile=DEFAULT_PLATEFILE, activate=True, name=None, do_snapshots=True, n_jobs=None)
    (options, args) = parser.parse_args()

    mm_jobset = create_mipmap_jobs(n_jobs=options.n_jobs, platefile=options.platefile, name=options.jobset_name)
    if options.do_snapshots:
        sn_jobset = create_snapshot_jobs(mmjobset=mm_jobset, platefile=options.platefile)
    else:
        sn_jobset = None
    if options.activate:
        for js in (mm_jobset, sn_jobset):
            if js:
                JobSet.activate(js)

if __name__ == '__main__':
    main()
