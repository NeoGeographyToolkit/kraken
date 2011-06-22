#!/usr/bin/env python
import sys, os, json
import csv
import re
import datetime
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
from pds.ingestion import cum_index


METADATA_DIR = '/big/sourcedata/mars/ctx/meta'
DEFAULT_PLATEFILE = 'pf://wwt10one/index/test_ctx_default.plate'
#transaction_id_sequence = Sequence('seq_transaction_id')

def gen_transaction_ids(initial=0):
    i = initial
    while True:
        i += 2
        yield i

def _build_mipmap_jobs(jobset, urls, platefile, n_jobs=None, options=None, initial_transaction_id=0, presave_callback=None):
    if options:
        downsample = options.downsample
        bandnorm = options.bandnorm
        clipping = options.clipping
    else:
        downsample = None
        bandnorm = False
        clipping = 0
    transaction_ids = gen_transaction_ids(initial_transaction_id)
    i = 0
    for url in Tracker(iter=urls, target=27859, progress=True):
        job = Job()
        job.transaction_id = transaction_ids.next()
        job.command = 'ctx2plate'
        job.arguments = job.wrapped().build_arguments(url=url, platefile=platefile, transaction_id=job.transaction_id, downsample=downsample)
        if bandnorm:
            job.arguments.append('--bandnorm')
        if not options.use_cache:
            job.arguments.append('--nocache')
        if options.use_percentages:
            job.arguments.append('--percentages')
        if options.no_plate:
            job.arguments.append('--noplate')
        job.arguments.append('--clipping=%f' % clipping)
        job.jobset = jobset
        if presave_callback:
            job = presave_callback(job)
        job.save()
        i += 1
        if n_jobs and i >= n_jobs: break
    print "Created %d jobs." % i

def generate_urls(metadata_dir=METADATA_DIR, baseurl='http://pds-imaging.jpl.nasa.gov/data/mro/mars_reconnaissance_orbiter/ctx/', is_test=False):
    if is_test:
        """ Read from the 1000 image sample around Valles Marineris. """
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
    else:
        """ Use the PDS Cumulative Index """
        tablefile = os.path.join(metadata_dir, 'cumindex.tab')
        labelfile = os.path.join(metadata_dir, 'cumindex.lbl')
        table = cum_index.Table(labelfile, tablefile)
        for row in table:
            if row.data_quality_desc.strip() == 'OK':  # filter for data quality errors
                if row.mission_phase_name.strip() in ('PSP','ESP'): # filter for mission phase
                    volume = row.volume_id.lower()
                    head, tail = os.path.split(row.file_specification_name)
                    head = head.lower()
                    url = os.path.join(baseurl, volume, head, tail)
                    yield url


@transaction.commit_on_success
def create_mipmap_jobs(n_jobs=None, platefile=DEFAULT_PLATEFILE, name=None, options=None):
    # where n_jobs is the number of jobs to generate.  Default (None) builds jobs for all assets in the queryset.
    #transaction_id_sequence.setval(1) # reset the transaction_id sequence
    jobset = JobSet()
    jobset.name = name or "CTX MipMap (%s)" % (n_jobs or 'all')
    jobset.command = "ctx2plate"
    jobset.priority = 3
    jobset.save()
    _build_mipmap_jobs(jobset, generate_urls(is_test=options.test), platefile, n_jobs=n_jobs, options=options)
    return jobset

@transaction.commit_on_success
def augment_jobset(jobset_id, platefile=DEFAULT_PLATEFILE, options=None):
    assert not options.test

    print "Retrieving JobSet %d..." % jobset_id
    jobset = JobSet.objects.get(id=jobset_id)

    print "Querying last transaction_id...",
    last_transaction_id = jobset.jobs.latest("transaction_id").transaction_id
    print str(last_transaction_id)

    print "Collecting product_ids already in jobset %d..." % jobset_id,
    existing_product_ids = set()
    existing_count = 0
    for job in jobset.jobs.only("context"):
        prod_id = job.arguments[0][-30:-4]
        existing_product_ids.add(prod_id)
        existing_count += 1
    print "done (%d jobs exist)." % existing_count

    def filter_urls(urls, id_set):
        product_id_pattern = re.compile('[A-Z0-9]{3}_\d{6}_\d{4}_X[IN]_\d{2}[NS]\d{3}W')
        for url in urls:
            product_id = product_id_pattern.search(url).group(0)
            assert product_id
            if product_id not in id_set:
                yield url

    new_urls = filter_urls(generate_urls(), existing_product_ids)

    def presave(job):
        job.context['aftermarket'] = True
        job.context['creation_date'] = str(datetime.date.today())
        return job
    
    _build_mipmap_jobs(jobset, new_urls, platefile, options=options, initial_transaction_id=last_transaction_id, presave_callback=presave)
    print "Added %d new jobs." % ( jobset.jobs.count() - existing_count )
    return jobset

def main():
    parser = optparse.OptionParser()
    parser.add_option('-p', '--platefile', action='store', dest='platefile', help='Platefile URL to which the images should be written (e.g. pf://wwt10one/index/collectible.plate)')
    parser.add_option('--njobs', action="store", dest="n_jobs", type="int", help="Limit the number of image2plate jobs generated")
    parser.add_option('--name', action='store', dest='jobset_name', help="Override the default name for the image2plate jobset.")
    parser.add_option('--no-activate', action='store_false', dest='activate', help='Do not activate the new jobsets after creation.')
    parser.add_option('--no-snapshots', action='store_false', dest='do_snapshots', help="Don't create a snapshot JobSet.")
    parser.add_option('--downsample', action='store', type='int', dest='downsample', help="Percentage to downsample during preprocessing.")
    parser.add_option('--bandnorm', action='store_true', dest='bandnorm', help="Perform ISIS band normalization.")
    parser.add_option('--clipping', action='store', type='float', dest='clipping', help="Clip to within N standard deviations of the mean intensity value (0 disables)")
    parser.add_option('--nocache', action='store_false', dest='use_cache', help='If there is a cached output cube, reprocess anyway')
    parser.add_option('--use-cache', action='store_true', dest='use_cache', help='Use a cached copy of the preprocessed output, if one exists.')
    parser.add_option('--percentages', dest='use_percentages', action='store_true', help="Use percentages instead of values for the stretch step (overrides clipping setting)")
    parser.add_option('--noplate', dest='no_plate', action='store_true', help="Skip platefile insertion.  Just preprocess.")
    parser.add_option('--test', dest='test', action='store_true', help="Draw image urls from a particular set of test images rather than the PDS cumulative label (default).")
    parser.add_option('--augment', dest='augment', action='store', type='int', help="Augment an existing jobset by creating new jobs only for products that do not already exist within the jobset.  Requires a jobset ID.")
    parser.set_defaults(
        platefile = DEFAULT_PLATEFILE, 
        activate = True, 
        name = None, 
        do_snapshots = True,
        n_jobs = None,  
        downsample = None, 
        bandnorm = False, 
        clipping = 3.0, 
        use_cache = False,
        use_percentages = False,
        test = False,
        augment = None,
        no_plate = False,
    )
    (options, args) = parser.parse_args()

    if options.augment:
        options.test = False
        options.activate = False
        options.do_snapshots = False
        augment_jobset(options.augment, platefile=options.platefile, options=options)
    else:
        mm_jobset = create_mipmap_jobs(n_jobs=options.n_jobs, platefile=options.platefile, name=options.jobset_name, options=options)
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
