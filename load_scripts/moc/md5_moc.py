import sys, os, glob
import md5

from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)
from ngt.assets.models import Asset
from ngt.utils.tracker import Tracker

rootpath='/big/sourcedata/moc'

def generate_volnames():
    for file in glob.glob(os.path.join(rootpath,'mgsc_1*')):
        if os.path.isdir(file):
            yield file
            
def generate_image_sums(md5file):
    f = open(md5file, 'r')
    for line in f:
        md5sum, filepath = line.split('  ')
        filepath = filepath.strip()
        if filepath[-4:] in ('.imq','.img'):
            yield filepath, md5sum.strip()
    f.close()
    
def getmd5(filename):
    f = open(filename, 'rb')
    md5sum = md5.new()
    while True:
        data = f.read(1024)
        if not data: break
        md5sum.update(data)
    return md5sum.hexdigest()
    
            
def main():
    for volname in Tracker(name="Volumes", iter=generate_volnames()):

        hashes = {}
        for filepath, md5sum in generate_image_sums("%s_md5.txt" % os.path.join(rootpath, volname)):
            file_name = '/'.join(filepath.split('/')[-2:]).upper()
            hashes[file_name] = md5sum
        assets = Asset.objects.filter(volume=volname.split('/')[-1].upper()).filter(md5_check=None)
        if assets.count() < 1:
            print "Nothing to hash."
            continue
        for asset in Tracker(name=volname, iter=assets.iterator(), target=assets.count(), progress=True):
            asset.md5_check = getmd5(asset.file_path) == hashes[asset.file_name]
            asset.save()

if __name__ == '__main__':
    main()
