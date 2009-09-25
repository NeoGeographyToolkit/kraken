try:
  from django.contrib.gis.db import models
except:
  from django.db import models # fallback for jython, since it doesn't have ctypes

from datetime import datetime

from pds.index_model import IndexProduct
from pds.ingestion.odymanager import OdyProductManager
from pds.ingestion.mgsmanager import MGSProductManager
from pds.ingestion.hirisemanager import HiriseProductManager
from pds.ingestion.cassinimanager import CassiniProductManager

from ngt.django_extras.fields import PickledObjectField

# the product model represents an individual image data product
class Product(models.Model):
    instrument_id = models.CharField(max_length=64)
    instrument_host_name = models.CharField(max_length=64, null=True)
    # drop constraint on observation_id column
    # ALTER TABLE pds_product ALTER COLUMN observation_id DROP NOT NULL;
    observation_id = models.CharField(max_length=64, null=True)
    product_id = models.CharField(max_length=64)
    dataset_id = models.CharField(max_length=64)
    target_name = models.CharField(max_length=64)
    footprint = models.PolygonField(null=True)
    resource_url = models.CharField(max_length=256, null=True)
    creation_time = models.DateTimeField()
    modification_time = models.DateTimeField()
    pds_label = PickledObjectField(null=True, db_column='pds_label') # python pickled metadata for the full PDS label
    
    # INDEX.TAB can contain vastly more information on a product than the product PDS label
    pds_index_row = PickledObjectField(null=True, db_column='pds_index_row') # this represents a row in the INDEX.TAB
    pds_index_table = models.ForeignKey(IndexProduct, db_column='pds_index_product_id', null=True)
    @property
    def pds_index_data(self):
        return dict(zip(self.pds_index_table.column_names, self.pds_index_row))

    # some additional metadata
    metadata = models.TextField(null=True)
    # added new status column, via
    # ALTER TABLE pds_product ADD COLUMN status varchar(64) DEFAULT 'ready';
    status = models.CharField(max_length=64, null=True, default='ready') # status of stored product, i.e. primer, updating, ready, ...

    # manually created a product_id search index for fast lookups
    # this one doesn't work??
    #CREATE INDEX pds_product_id_index ON "pds_product" (lower("pds_product"."product_id") varchar_pattern_ops);
    # but this one does!
    #CREATE INDEX pds_product_id_index ON "pds_product" ("product_id");
    #DROP INDEX pds_product_id_index;
    # debugging
    # explain select "dataset_id", "product_id", "id", "footprint" from "pds_product" WHERE "product_id" = 'I25665003RDR' LIMIT 1;

    objects = models.GeoManager()
    hirise_objects = HiriseProductManager()
    mgs_objects = MGSProductManager()
    ody_objects = OdyProductManager()
    co_objects = CassiniProductManager()

    def __unicode__(self):
      return u'%s:%s' % (self.product_id, self.instrument_id)

    def __str__(self):
      return "%s - %s" % (self.__unicode__(), self.id or 'new')

    def save(self, *args, **kwargs):
        now = datetime.now()
        if not self.id or Product.objects.filter(id=self.id).count() == 0:
            self.creation_time = now
        self.modification_time = now
        #super(Product,self).save(self, *args, **kwargs)
        models.Model.save(self, *args, **kwargs)

class Asset(models.Model):
    #pds_product = models.ForeignKey(Product, null=True)
    products = models.ManyToManyField(Product, related_name='assets')
    parents = models.ManyToManyField(Asset, related_name='children')
    #original = models.ForeignKey('Asset', null=True)
    is_original = models.BooleanField(default=False)
    image_path = models.FilePathField(max_length=4096) #4096 being linux's maximum absolute path length
    name = model.TextField(max_length=512, null=True)   

def asset_verify_originality(instance, **kwargs):
    if instance.is_original: #'''another original exists with the same pds product''':
        qs = Asset.objects.filter(pds_product=instance.pds_product).filter(is_original=True)
        if qs.count > 0:
            if qs.count() > 1 or qs[0] != instance:
                raise Exception("Tried to add a duplicate original asset for product %s" % instance.pds_product)
models.signals.pre_save.connect(asset_verify_originality, sender=Asset)
