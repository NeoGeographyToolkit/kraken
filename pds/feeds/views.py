from urllib import unquote_plus
from datetime import datetime
from django.http import HttpResponse, HttpResponseNotFound

try:
  import yaml
except ImportError:
  pass # implement a different solution for jython
try:
  import jsonlib as json
except ImportError:
  try:
    import json
  except ImportError:
    pass # do something about it!

from pds.models import Product

def json_datetime_handler(value):
  if isinstance(value, datetime): return str(value)
  raise json.UnknownSerializerError

def metadata(request, instrument_id=None, dataset_id=None, product_id=None, format='yaml', meta_column='label'):

  try:
    if instrument_id is not None:
      product = Product.objects.filter(instrument_id=unquote_plus(instrument_id), product_id=unquote_plus(product_id))[0]
    elif dataset_id is not None:
      product = Product.objects.filter(dataset_id=unquote_plus(dataset_id), product_id=unquote_plus(product_id))[0]
    else:
      raise

    if meta_column == 'label':
      metadata = product.pds_label
    else:
      metadata = product.pds_index_data

    if format == 'yaml':
      result = yaml.safe_dump(metadata)
    elif format == 'json':
      result = json.dumps(metadata, on_unknown = json_datetime_handler)
    else:
      raise
      # TODO: other formats, i.e. json, xml

    response = HttpResponse(result, 'text/plain')
  except:
    response = HttpResponseNotFound('Product not found!')

  return response
