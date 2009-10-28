from django.contrib.gis.db import models
from pds.models import Product

DATA_ROOT = '/big/sourcedata/MOC'
class Asset(models.Model):
    '''  Image file asset -- built for MOC data '''
    volume = models.TextField(max_length=256, null=True)
    product_id = models.TextField(max_length=512, null=True) # only meaningful if this asset is associated with a single product
    products = models.ManyToManyField(Product, related_name='ngt_assets')
    parents = models.ManyToManyField('Asset', symmetrical=False, related_name='children')
    is_original = models.BooleanField(default=False)
    file_name = models.FilePathField(max_length=4096) #4096 being the linux kernel's default maximum absolute path length
    name = models.TextField(max_length=512, null=True)
    
    status = models.TextField(max_length=128, null=True)
    md5_check = models.BooleanField(null=True)
    class_label = models.TextField(max_length=512, null=True)
    
    def __unicode__(self):
        if self.name:
            rep = self.name
        elif self.pds_product_id:
            rep = self.pds_product_id
        else:
            rep = self.id
        return "<Asset: %s>" % rep

    @property
    def file_path(self, dataroot=DATA_ROOT):
        return os.path.join(dataroot, self.volume, self.file_name)
