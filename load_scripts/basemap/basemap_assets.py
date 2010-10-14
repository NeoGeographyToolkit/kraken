from assets.models import Asset
import os
from django.db import transaction

@transaction.commit_on_success
def create_assets():
    sourcedir = '/big/assets/mars_color_merge'
    files = os.listdir(sourcedir)
    assert all(['.tif' in f for f in files])

    ids = []
    for filename in files:
        print "creating asset for %s" % filename
        a = Asset()
        a.relative_file_path = 'mars_color_merge/' + filename
        a.class_label= 'color basemap'
        a.save()
        ids.append(a.id)
    return ids
