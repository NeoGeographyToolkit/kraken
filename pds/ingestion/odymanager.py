import sys
try:
  import yaml
except ImportError:
  pass # implement a different solution for jython
from datetime import datetime
from cStringIO import StringIO

from django.db.models import Q
from django.db import connection, transaction

try:
        from django.contrib.gis.geos.base import GEOSGeometry #Django 1.0.2
        #from django.contrib.gis.geos.geometries import GEOSGeometry #Django 1.1
except ImportError::
        from django.contrib.gis.geos.geometry import GEOSGeometry #Django 1.1
#from django.contrib.gis.geos.geometries import Polygon

from basemanager import BaseManager

class OdyProductManager(BaseManager):

  # a Product model - Themis yaml translation dictionary
  filterkeys = {
    'dataset_id': 'data_set_id',
    'product_id': 'product_id',
    'target_name': 'target_name',
    'instrument_id': 'instrument_id',
    'observation_id': 'product_id',
  }

  # return only ODY data specific data products
  def get_query_set(self):
    return super(OdyProductManager,self).get_query_set().filter(Q(dataset_id__startswith='ODY'))

  def get_duplicates_by_product_id(self):
    cursor = connection.cursor()
    cursor.execute("""
      SELECT product_id, COUNT(product_id) AS NumOccurrences
      FROM pds_product
      WHERE dataset_id LIKE %s
      GROUP BY product_id HAVING ( COUNT(product_id) > 1 )""", ['ODY%'])
    rows = cursor.fetchall() # this is hopefully a short list
    return rows

  def digest_yaml(self, fobj):
    yaml_data = fobj.read() # this is yaml serialized data
    array_of_hashes = yaml.load(yaml_data)

    # extract core Product model values
    hash = array_of_hashes # this is a simple case
    product_id = hash['product_id']
    model_hash = dict([ ('resource_url', hash.get('resourceURL')[0]), ('pds_label',hash), \
      ('metadata', '') ])

    values = [(f, hash.get(self.filterkeys.get(f.name, ''), None) \
      or model_hash.get(f.name, None) ) for f in self.model._meta.fields \
      if self.filterkeys.has_key(f.name) or model_hash.has_key(f.name) ]

    return (product_id, values)

  @transaction.commit_manually
  def ingest_footprint(self, logFile=None, force=False):
    if self.csv_file is None:
      return
    if logFile is not None:
      try:
        logging = open(logFile,'w')
      except IOError:
        logging = sys.stdout
    try:
      # filter out update fields
      meta_field = self.model._meta.get_field_by_name('metadata')[0]
      geom_field = self.model._meta.get_field_by_name('footprint')[0]
      # start interation
      counter = 0
      for line in self.csv_file:
        counter = counter + 1
        fields = line.split(',')
        product_id = '%sRDR' % fields[0]
        entries = self.get_query_set().filter(product_id=product_id)
        if len(entries)>1:
          print >> logging, '%s, multiple entries found!' % product_id
          continue
        elif len(entries)==1:
          entry = entries[0]
          if entry.footprint is not None and not force:
            continue
          # what serialized format, yaml, json, pickle?
          phase, incidence, emission = fields[9], fields[10], fields[11]
          additional_meta = StringIO()
          #additional_meta.write(entry.metadata)
          additional_meta.write(\
          '- {centerphaseangle: %s,centerincidenceangle: %s,centeremissionangle: %s}\n' % (phase,incidence,emission))
          # tuples of longitude & latitude
          geom = GEOSGeometry('POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))' \
            % tuple(fields[1:9]+fields[1:3]) )
          # save entry via _update uses field objects instead of field names (field, model, val)
          values = [ (meta_field, None, meta_field.get_db_prep_save(additional_meta.getvalue())),
            (geom_field, None, geom_field.get_db_prep_save(geom)) ]
          #print values
          self.get_query_set().filter(pk=entry.pk)._update(values)
          transaction.commit()
          # stderr is unbuffered!
          sys.stderr.write('\r%d - %s, %d' % (counter, product_id, entry.pk))
        else:
          print >> logging, '%s, is missing in database' % product_id
          continue

    finally:
      if logFile is not None:
        logging.close()
      self.csv_file.close()

if __name__ == "__main__":
  from pds.models import Product

  yaml_repository = 'data/odyyaml.tar.bz2'
  csv = 'data/odyGeo.csv.bz2'

  Product.ody_objects.initialize(yaml_repository, csv)
  Product.ody_objects.ingest()
  Product.ody_objects.ingest_footprint('missing.log')
  # we should be done here!
  print 'done!'

  sys.exit()

# >>> from pds.models import Product
# >>> yaml_repository = 'data/themis_yaml.tar.bz2'
# >>> Product.ody_objects.initialize(yaml_repository)
# >>> Product.ody_objects.ingest()
