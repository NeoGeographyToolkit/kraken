import sys, os
sys.path.insert(0, '../../..')
from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)

from ngt.assets.models import Asset
import moc_stage

"""
testids = [
    'S21/01438',
    'M01/04564',
    'E01/01883',
    'M22/01571',
    'M21/00236',
    'M22/02026',
    'M01/04331',
    'S21/01071',
    'E01/01402',
    'S21/00337',
]
"""
testids = [
    "M03/05609",
    "M03/05590",
    "M03/05571",
    "M03/05561",
    "M03/05576",
    "M03/05586",

]
count = 0
for id in testids:
    count += 1
    print "Start ", count
    a = Asset.objects.get(product_id=id)
    try:
        #moc_stage.stage_image(a.file_path, output_dir='testdata/')
        outfile = 'out/'+os.path.splitext(os.path.basename(a.file_path))[0]+'.cub'
        moc_stage.mocproc(a.file_path, outfile, map='PolarStereographic')
    except AssertionError, e:
        print e
    print "Finish ", count
