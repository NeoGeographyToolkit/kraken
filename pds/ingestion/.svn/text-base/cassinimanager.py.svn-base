try:
  import yaml
except ImportError:
  pass # implement a different solution for jython
from datetime import datetime

from django.db.models import Q
from django.db import connection, transaction

from basemanager import BaseManager

class CassiniProductManager(BaseManager):

  # a Product model - Hirise yaml translation dictionary
  filterkeys = {
    'dataset_id': 'data_set_id',
    'product_id': 'product_id',
    'target_name': 'target_name',
    'instrument_id': 'instrument_id',
    'observation_id': 'product_id',
    'instrument_host_name': 'instrument_host_name'
  }

  def get_query_set(self):
    return super(CassiniProductManager,self).get_query_set().filter(instrument_host_name='CASSINI ORBITER')

  def digest_yaml(self, fobj):
    yaml_data = fobj.read() # this is yaml serialized data
    array_of_hashes = yaml.load(yaml_data)

    # extract core Product model values
    hash = array_of_hashes # this is a simple case
    product_id = hash['product_id']
    model_hash = dict([ ('resource_url', hash.get('resourceURL')[0]), ('pds_label', hash), \
      ('metadata', '') ])

    values = [(f, hash.get(self.filterkeys.get(f.name, ''), None) \
      or model_hash.get(f.name, None) ) for f in self.model._meta.fields \
      if self.filterkeys.has_key(f.name) or model_hash.has_key(f.name) ]

    return (product_id, values)

  def primer_from_index_row(self, row, value_list):
    value_list.append((self.product_id_field, row.product_id))
    value_list.append((self.dataset_id_field, row.data_set_id))
    value_list.append((self.instrument_id_field, row.instrument_id))
    value_list.append((self.instrument_host_name_field, row.instrument_host_name))
    value_list.append((self.target_name_field, row.target_name))
    value_list.append((self.observation_id_field, row.observation_id))
    fname  = row.file_specification_name # in hirise index data it is called file_name_specification
    volume = row.volume_id
    if not fname.endswith('LBL'):
      fname = fname[:-3] + 'LBL'
    value_list.append((self.resource_url_field, 'http://pds-imaging.jpl.nasa.gov/data/cassini/cassini_orbiter/%s/%s' % (volume.lower(), fname)))

if __name__ == "__main__":
  from pds.models import Product

  #yaml_repository = 'data/hirise_yaml.tar.bz2'

  Product.co_objects.initialize(yaml_repository)
  Product.co_objects.ingest_primer(force=True)
  # we should be done here!
  print 'done!'

  sys.exit()

# >>> from pds.models import Product
# >>> primer_csv = 'data/cassini_atlas_report.csv.bz2'
# >>> yaml_repository = 'data/cassini_yaml.tar.bz2'
# >>> Product.co_objects.initialize(yaml_repository, csv_file=primer_csv)
# >>> Product.co_objects.ingest_primer(force=True)
# ingest the yaml repository
# >>> Product.co_objects.ingest(force=True)

# >>> Product.co_objects.filter(status='sync').remotesync(batchsize=50)
# ingest cumulative index table:
# >>> Product.co_objects.ingest_index('data/COISS_2004.LBL.gz','data/COISS_2004.TAB.bz2','http://pds-imaging.jpl.nasa.gov/data/cassini/cassini_orbiter/coiss_2004/index/index')
