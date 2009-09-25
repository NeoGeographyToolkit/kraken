from django.db import models

from datetime import datetime

from ngt.django_extras.fields import PickledObjectField

# the index product model represents additional INDEX data per PDS volumne
class IndexProduct(models.Model):
    column_names = PickledObjectField(null=True, db_column='column_names')
    # this serves as a unique id
    resource_url = models.CharField(max_length=256)
    creation_time = models.DateTimeField()
    modification_time = models.DateTimeField()

    class Meta:
      db_table = 'pds_index_product'

    def __unicode__(self):
      return u'%s' % self.resource_url

    def __str__(self):
      return "%s - %s" % (self.__unicode__(), self.id or 'new')

    def save(self, *args, **kwargs):
        now = datetime.now()
        if not self.id or IndexProduct.objects.filter(id=self.id).count() == 0:
            self.creation_time = now
        self.modification_time = now
        #super(Product,self).save(self, *args, **kwargs)
        models.Model.save(self, *args, **kwargs)
