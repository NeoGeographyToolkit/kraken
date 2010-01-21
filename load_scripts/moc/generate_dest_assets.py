import sys, os, json, os.path
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

from psycopg2 import IntegrityError

ROOTPATH='/big/assets/mocsource/'
DESTPATH='/big/assets/moc/'


@transaction.autocommit
def main():
    errlog = open('err.log', 'w')
    JOBSET = 3
    jobs = Job.objects.filter(jobset__id=JOBSET, status_enum=Job.StatusEnum.COMPLETE)
    failures = 0
    for job in Tracker(iter=jobs):
        asset_o = job.assets.all()[0]
        try:
                asset_n = copy(asset_o)
                asset_n.id = None
                asset_n.is_original = False
                args = json.loads(job.arguments)
                assert os.path.exists(args[1])
                asset_n.relative_file_path = args[1].replace('/big/assets/','')
                #import pdb; pdb.set_trace()
                assert asset_n.relative_file_path[0:4] == 'moc/'
                asset_n.class_label = "mocprocd image"
                asset_n.save()
                job.assets.add(asset_n)
                asset_n.parents.add(asset_o)
        except Exception as exc:
            failures += 1
            errlog.write(str(exc))
            continue
    errlog.close()
    print "Done."
    if failures > 0:
        print "There were %d failures.  Check err.log." % failures
        
def delete_cruft():
    print "DELETING MOCPROCD ASSETS"
    Asset.objects.filter(class_label="mocprocd moc image").delete()
    print "Done."
        
if __name__ == '__main__':
    main()
