import sys
sys.path.insert(0, '../../..')
from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)

from ngt.assets.models import Asset
import moc_stage

testids = [
    'S21/01438',
    'M01/04564',
    'E01/01883',
    'M22/01571',
    'M21/00236',
]

for id in testids:
    a = Asset.objects.get(product_id=id)
    moc_stage.stage_image(a.file_path, output_dir='testdata/')
