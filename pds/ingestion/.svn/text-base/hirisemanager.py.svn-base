try:
  import yaml
except ImportError:
  pass # implement a different solution for jython

from django.db.models import Q
from django.db import connection, transaction

from basemanager import BaseManager

class HiriseProductManager(BaseManager):

  # a Product model - Hirise yaml translation dictionary
  filterkeys = {
    'dataset_id': 'data_set_id',
    'product_id': 'product_id',
    'target_name': 'target_name',
    'instrument_id': 'instrument_id',
    'observation_id': 'observation_id',
  }

  def get_query_set(self):
    return super(HiriseProductManager,self).get_query_set().filter(instrument_id='HIRISE')

  def digest_yaml(self, fobj):
    yaml_data = fobj.read() # this is yaml serialized data
    hash = yaml.load(yaml_data)

    # extract core Product model values
    product_id = hash['product_id']
    model_hash = dict([ ('resource_url', hash.get('resourceURL')[0]), ('pds_label', hash), \
      ('metadata', '') ])

    values = [(f, hash.get(self.filterkeys.get(f.name, ''), None) \
      or model_hash.get(f.name, None) ) for f in self.model._meta.fields \
      if self.filterkeys.has_key(f.name) or model_hash.has_key(f.name) ]

    return (product_id, values)

  def primer_from_index_row(self, row, value_list):
    value_list.append((self.product_id_field, row.product_id))
    value_list.append((self.dataset_id_field, 'MRO-M-HIRISE-3-RDR-V1.0'))
    value_list.append((self.instrument_id_field, row.instrument_id))
    value_list.append((self.target_name_field, row.target_name))
    value_list.append((self.observation_id_field, row.observation_id))
    fname = row.file_name_specification
    if fname.endswith('JP2'):
      fname = fname[:-3] + 'LBL'
    value_list.append((self.resource_url_field, 'http://hirise-pds.lpl.arizona.edu/PDS/%s' % fname))

if __name__ == "__main__":
  from pds.models import Product

  yaml_repository = 'data/hirise_yaml.tar.bz2'

  Product.hirise_objects.initialize(yaml_repository)
  Product.hirise_objects.ingest(force=True)
  # we should be done here!
  print 'done!'

  sys.exit()

# >>> from pds.models import Product
# >>> yaml_repository = 'data/hirise_yaml.tar.bz2'
# >>> Product.hirise_objects.initialize(yaml_repository)
# >>> Product.hirise_objects.ingest(force=True)
# find the latest product
# >>> p = Product.hirise_objects.latest('modification_time')
# update metadata labels:
# >>> Product.hirise_objects.all().remotesync(batchsize=100)
# ingest cumulative index table:
# >>> Product.hirise_objects.ingest_index('data/RDRCUMINDEX.LBL','data/RDRCUMINDEX.TAB.bz2','http://hirise-pds.lpl.arizona.edu/PDS/INDEX/RDRCUMINDEX')
