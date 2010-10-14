import subprocess
from assets.models import Asset
from django.db import transaction

ASSET_BASEDIR = '/big/assets/'
HRSC_BASEDIR = '/big/assets/hrsc/'

MOLA_BASEMAP_PATH = '/big/assets/mola/megt128.tif'

def get_image_paths():
    p = subprocess.Popen('/usr/bin/find %s -name *_dt4.img*' % HRSC_BASEDIR, shell=True, stdout=subprocess.PIPE)
    output = p.communicate()[0]
    for fname in output.split("\n"):
        if fname.strip():
            yield fname

def new_asset(class_label, path):
    relative_path = path.partition(ASSET_BASEDIR)[-1]
    a = Asset(class_label=class_label, relative_file_path=relative_path)
    a.is_original = True
    a.save()

@transaction.commit_on_success
def create_assets():
    i = 0
    for imgpath in get_image_paths():
        new_asset('hrsc', imgpath)
        i += 1

    print "%d assets created." % i

@transaction.commit_on_success
def add_mola():
    a = Asset(class_label='mola basemap')
    a.relative_path = MOLA_BASEMAP_PATH.partition(ASSET_BASEDIR)[-1]
    a.is_original = True
    a.save()
