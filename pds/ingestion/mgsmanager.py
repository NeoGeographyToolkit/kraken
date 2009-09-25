try:
  import yaml
except ImportError:
  pass # implement a different solution for jython

from django.db.models import Q
from django.db import connection, transaction

from basemanager import BaseManager

class MGSProductManager(BaseManager):

  # a Product model - MGS yaml translation dictionary
  filterkeys = {
    'dataset_id': 'data_set_id',
    'product_id': 'product_id',
    'target_name': 'target_name',
    'instrument_id': 'instrument_id',
    'observation_id': 'product_id',
  }

  # return only ODY data specific data products
  def get_query_set(self):
    return super(OdyProductManager,self).get_query_set().filter(Q(dataset_id__startswith='MGS'))

  def get_duplicates_by_product_id(self):
    cursor = connection.cursor()
    cursor.execute("""
      SELECT product_id, COUNT(product_id) AS NumOccurrences
      FROM pds_product
      WHERE dataset_id LIKE %s
      GROUP BY product_id HAVING ( COUNT(product_id) > 1 )""", ['MGS%'])
    rows = cursor.fetchall() # this is hopefully a short list
    return rows

  def digest_yaml(self, fobj):
    yaml_data = fobj.read() # this is yaml serialized data
    array_of_hashes = yaml.load(yaml_data)

    # extract core Product model values
    hash = array_of_hashes[0] # this is a simple case
    product_id = hash['product_id']
    model_hash = dict([ ('resource_url', hash.get('resourceURL')[0]), ('metadata', yaml_data) ])

    values = [(f, hash.get(self.filterkeys.get(f.name, ''), None) \
      or model_hash.get(f.name, None) ) for f in self.model._meta.fields \
      if self.filterkeys.has_key(f.name) or model_hash.has_key(f.name) ]

    return (product_id, values)

if __name__ == "__main__":

  from pds.models import Product

  yaml_repository = '../data/mgsyaml.tar.bz2'

  Product.mgs_objects.initialize(yaml_repository)
  Product.mgs_objects.ingest()
  # we should be done here!
  print 'done!'

  sys.exit()
