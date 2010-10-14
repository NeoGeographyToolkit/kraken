from ngt import settings
if not settings.DISABLE_GEO:
        from django.contrib.gis.db import models
        #from pds.models import Product
else:
    from django.db import models
import os

#DATA_ROOT = '/big/sourcedata/moc'
DATA_ROOT = '/big/assets/'
class Asset(models.Model):
    '''  Image file asset -- built for MOC data '''
    volume = models.TextField(max_length=256, null=True)
    product_id = models.TextField(max_length=512, null=True) # only meaningful if this asset is associated with a single product

    parents = models.ManyToManyField('Asset', symmetrical=False, related_name='children')
    is_original = models.BooleanField(default=False)
    relative_file_path = models.FilePathField(max_length=4096) #4096 being the linux kernel's default maximum absolute path length
    name = models.TextField(max_length=512, null=True)
    
    status = models.TextField(max_length=128, null=True)
    md5_check = models.BooleanField(null=True)
    has_errors = models.BooleanField(default=False)
    class_label = models.TextField(max_length=512, null=True)
    creator_job = models.ForeignKey('jobs.Job', null=True, related_name='output_assets')

    # MOC specific stuff
    instrument_name = models.TextField(max_length=128, null=True)
    center_latitude = models.FloatField(null=True)
    min_latitude = models.FloatField(null=True)
    max_latitude = models.FloatField(null=True)
    if not settings.DISABLE_GEO:
            footprint = models.PolygonField(null=True, srid=949900)
    
    def __unicode__(self):
        if self.name:
            rep = self.name
        elif self.product_id:
            rep = self.product_id
        else:
            rep = self.id
        return "<Asset: %s>" % rep

    @property
    def file_path(self, dataroot=DATA_ROOT):
        """ Read only attribute: to set the file path, use relative_file_path """
        #return os.path.join(DATA_ROOT, self.volume.lower(), self.file_name.lower())
        return os.path.join(DATA_ROOT, self.relative_file_path)

from ngt.jobs.models import Job # this needs to come after the Asset class def to avoid a circular import
