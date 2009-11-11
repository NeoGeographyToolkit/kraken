import urllib
import re, os, sys
from BeautifulSoup import BeautifulSoup as Soup
sys.path.insert(0, '/home/ted/alderaan-wc/')
from ngt.utils.tracker import Tracker


rooturl = 'http://pds-imaging.jpl.nasa.gov/data/mgs-m-moc-na_wa-2-sdp-l0-v1.0/'
targetpath = '/home/ted/data/moc_meta'
indexfiles = ['imgindx.lbl','imgindx.tab','imgindex.lbl','imgindex.tab']

root = urllib.urlopen(rooturl)
soup = Soup(root.read())
volpattern = re.compile('^mgsc_\d+/?$')
dirlinks = soup.findAll('a', href=volpattern)

for voldir in Tracker(iter=[l['href'] for l in dirlinks] ):
    try:
        target_dir = os.path.join(targetpath, voldir, 'index')
        os.makedirs(target_dir)
    except os.error:
        pass

    for ifile in indexfiles:
        img_response = urllib.urlopen(rooturl + voldir + 'index/' + ifile)
        if img_response.getcode() == 200:
            out = open(os.path.join(target_dir, ifile), 'w')
            out.write(img_response.read())
            out.close()
