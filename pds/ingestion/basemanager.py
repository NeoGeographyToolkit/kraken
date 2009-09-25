from cStringIO import StringIO
import cPickle as pickle
import tarfile
try:
  from bz2 import BZ2File
except ImportError:
  pass # implement a different solution for jython
import gzip
from datetime import datetime
from tempfile import mkdtemp
from subprocess import Popen, PIPE
import os

from django.contrib.gis.db import models
from django.db.models import Q
#from django.db.models.query_utils import CollectedObjects
from django.db import connection, transaction

# for multithreaded processing
from pds.ingestion import pool, db_committing_queue
from asyncjoin import async

# for ingesting PDS Index tables
from pds.index_model import IndexProduct
from cum_index import Table

# Used to control how many objects are worked with at once in some cases (e.g.
# when updating objects).
CHUNK_SIZE = 200

# reference location to the script directory
this_dir = os.path.dirname(__file__)
SCRIPT_DIR = os.path.abspath(this_dir+'/../scripts')

# TODO: make this customizable for specific data sets
def record_remotesync_update(manager, productId, hash):
  global db_committing_queue

  try:
    products = manager.filter(product_id=productId, status='sync')
    if len(products) == 1:
      # print "update product with id '%s'" % productId
      product = products[0]
      product.observation_id = hash.get('observation_id', hash.get('image_number', productId))
      product.instrument_id = hash.get('instrument_id', '')
      product.instrument_host_name = hash.get('instrument_host_name', hash.get('spacecraft_name', ''))
      product.dataset_id = hash.get('data_set_id', '')
      product.target_name = hash.get('target_name', '')
      filename, filesize = hash['resourceURL'][0:2]
      base, sep, ext = filename.rpartition('.')
      hash['resourceURL'] = [product.resource_url, filesize, ext]
      product.pds_label = hash # PickledObjectField automatically pickles objects
      product.status = 'ready'
      db_committing_queue.put(product, True) # blocking put
      #don't save here
    elif len(products) > 1:
      print "multiple products with id '%s' match" % productId
    else:
      print "no product %s to sync!" % productId
  except Exception, e:
    if repr(e).find('OperationalError') > -1:
      db_commiting_queue.put(product, True) # blocking put
    else:
      raise
  finally:
    pass

# TODO: make this Exception robust!
@async(threadpool=pool)
def fetchResources(collected_objects, product_model):

  if collected_objects is None:
    return
  # download remote sources
  tempdir = mkdtemp()
  #print 'temporary directory', tempdir
  f = None
  try:
    f = open('%s/urls.csv' % tempdir, 'w')
    for item in collected_objects:
      f.write('%s\n' % str(item[0]))
  finally:
    if f is not None:
      f.close()

  process = None
  pickle_stream = None
  counter = 0
  try:
    process = Popen(["processlabel.bash", tempdir, SCRIPT_DIR], bufsize=-1, stdin=PIPE, \
      stdout=PIPE, stderr=PIPE, env={"PATH": "/bin:/usr/bin:%s" % SCRIPT_DIR})
    print 'new child process', process.pid
    # ingest back into the database, this thread blocks till subprocess has exited!
    pickle_stream = StringIO(process.stdout.read())
    # TODO: implement expressive diagnostics on the single product level!
    try:
      f = open('%s.log' % tempdir, 'w')
      if f is not None:
        f.write(process.stderr.read())
        f.close()
    except:
      pass

    # iterate over results
    manager = product_model.objects
    for cursor_line in pickle_stream:
      pickled_object_size = int(cursor_line.rstrip('\n'))
      pickled_object = pickle_stream.read(pickled_object_size)
      counter = counter + 1
      try:
        # instantiate python object
        hash = pickle.loads(pickled_object)
        # update model in DB with new data in simplistic django style. TODO: maybe speed up?
        productId = hash['product_id']
        record_remotesync_update(manager, productId, hash)
      except Exception, e:
        print repr(e)
        continue

  except Exception, e:
    print "fetchResources: %s" % repr(e)

  finally:
    # clean up
    print "inserted %d elements" % counter
    if pickle_stream is not None:
      pickle_stream.close()
    if process is not None:
      process.stdin.close()
      process.stdout.close()
      process.stderr.close()

# augment query set methods by subclassing
# this allows the following syntax:
# Product.objects.all().filter(...).remotesync()
# Hence, we can conveniently filter for products and update them automatically
class CustomQuerySet(models.query.QuerySet):

  # crawl url and retrieve remote label files, then ingest
  def remotesync(self, batchsize=CHUNK_SIZE):
    """
    Update all elements in the current QuerySet from the remote source, setting all the given
    fields to the appropriate values.
    """

    print "starting remotesync: (batchsize=%d)" % batchsize
    # query only selected fields, can be huge:
    sync_query = super(CustomQuerySet, self).values_list('resource_url', 'product_id')
    num_entries = len(sync_query) # execute query!!
    if num_entries == 0:
      print "nothing to synchronize - bye!"
      return
    else:
      print "a total of %d entries need to be synchronized." % num_entries

    # Synchronize objects in chunks to prevent a single process to become too big.
    # set status column of selected elements to 'sync' status
    super(CustomQuerySet, self).update(status='sync')
    offset = 0
    while offset < num_entries:
      try:
        collection = sync_query[offset:offset+batchsize]
        if len(collection) > 0:
          print "queue request at offset %d with batch size %d" % (offset, len(collection))
          # TODO: async queue doesn't seem to take the buffering capacity into account
          fetchResources(collection, self.model)
      except:
        continue
      finally:
        offset = offset + batchsize

    # Clear the result cache, in case this QuerySet gets reused.
    self._result_cache = None
  remotesync.alters_data = True

class BaseManager(models.Manager):

  def __init__(self):
    #self.super_manager = Product._default_manager
    self.queryset_class = CustomQuerySet
    super(BaseManager,self).__init__()

  def set_model_fields(self):
    # quick field access
    self.modtime_field = self.model._meta.get_field_by_name('modification_time')[0]
    self.createtime_field = self.model._meta.get_field_by_name('creation_time')[0]
    self.pds_index_row_field = self.model._meta.get_field_by_name('pds_index_row')[0]
    self.pds_index_table_field = self.model._meta.get_field_by_name('pds_index_table')[0]
    self.status_field = self.model._meta.get_field_by_name('status')[0]
    self.resource_url_field = self.model._meta.get_field_by_name('resource_url')[0]
    self.observation_id_field = self.model._meta.get_field_by_name('observation_id')[0]
    self.instrument_host_name_field = self.model._meta.get_field_by_name('instrument_host_name')[0]

    # mandatory fields for the primer
    self.product_id_field = self.model._meta.get_field_by_name('product_id')[0]
    self.dataset_id_field = self.model._meta.get_field_by_name('dataset_id')[0]
    self.instrument_id_field = self.model._meta.get_field_by_name('instrument_id')[0]
    self.target_name_field = self.model._meta.get_field_by_name('target_name')[0]

  def get_query_set(self):
    return self.queryset_class(self.model)

  def initialize(self, yaml_repository=None, csv_file=None):
    if yaml_repository:
      try:
        self.tarrepos = tarfile.open(yaml_repository,'r:*')
      except tarfile.ReadError:
        print 'Tar archive, %s, cannot be opened!' % yaml_repository
        self.tarrepos = None
    if csv_file:
      try:
        self.csv_file = BZ2File(csv_file,'r')
      except IOError:
        print 'csv file, %s, cannot be opened!' % csv_file
        self.csv_file = None

  # needs to be overridden
  def get_duplicates_by_product_id(self):
    return []

  # get rid of duplicates
  def remove_duplicates(self, mode='manual'):
    # automatically removes duplicate entries in DB
    duplicate_products = self.get_duplicates_by_product_id()
    for product in duplicate_products:
      print product
      # TODO: write removal procedure

  # needs to be overridden
  def digest_yaml(self, fobj):
    pass

  # batch commit to DB, that's much faster!!
  # TODO make ingestion more robust!!
  @transaction.commit_manually
  def ingest(self, batchsize=50, offset=0, force=False):
    if self.tarrepos is None:
      return

    # set quick access model fields:
    self.set_model_fields()

    try:
      counter = 0
      batch_counter = 0
      self.now = datetime.now()
      member = self.tarrepos.next()
      while member:
        if member.isreg() and member.name.endswith('yaml'):
          counter = counter + 1
          if counter < offset:
            continue
          try:
            product_id, values = self.digest_yaml(self.tarrepos.extractfile(member))
            if product_id is None:
              continue

            existing = self.get_query_set().filter(product_id=product_id)

            if values is not None:
              # Create a new record.
              #self.super_manager._insert(values, return_id=True)
              entries = len(existing)
              values.append((self.modtime_field, self.now))
              if entries == 0:
                batch_counter = batch_counter + 1
                values.append((self.createtime_field, self.now))
                # values is a list of (field, field value) pairs
                insert_values = [(f, f.get_db_prep_save(val)) for (f, val) in values]
                self._insert(insert_values, return_id=True) 
              elif entries == 1 and force:
                batch_counter = batch_counter + 1
                # values must be a list of (field, model, field value) triples!
                update_values = [(f, None, f.get_db_prep_save(val)) for (f, val) in values]
                self.get_query_set().filter(pk=existing[0].pk)._update(update_values)
              else:
                pass
                # report the error
              # commit to DB a batch of 100 insert records
              if (batch_counter >= batchsize):
                transaction.commit()
                self.now = datetime.now() # update now statement for every batch submission
                print "committed batch of size %d from position %d" % (batch_counter, counter)
                batch_counter = 0
              elif (counter % 500) == 0:
                print "processed %d documents" % counter

          except Exception, e:
            # debug report
            print repr(e)
            print connection.queries[-1]

        member = self.tarrepos.next()

    finally:
      # before exiting commit the last batch to DB,
      # is this necessary with the decorator?
      transaction.commit()
      self.tarrepos.close()

  # needs to be overridden
  def primer_from_index_row(self, row, value_list):
    pass

  # batch commit to DB, that's much faster!!
  # TODO make ingestion more robust!!
  @transaction.commit_manually
  def ingest_index(self, index_lbl, index_tab, index_table_id, batchsize=50, offset=0, limit=None, force=False):
    if index_table_id is None:
      print 'need unique index table id, it\'s resource url! - bye.'
      return
    # open file handles for index files
    try:
      name, sep, ext = index_lbl.lower().rpartition('.')
      # try gzip files first
      if ext == 'gz':
        lbl_fd = gzip.open(index_lbl, 'r')
      elif ext == 'bz2':
        lbl_fd = BZ2File(index_lbl, 'r')
      else:
        lbl_fd = open(index_lbl, 'r')
    except:
      print 'no index label file present! - bye.'
      return
    try:
      name, sep, ext = index_tab.lower().rpartition('.')
      # try gzip files first
      if ext == 'gz':
        tab_fd = gzip.open(index_tab, 'r')
      elif ext == 'bz2':
        tab_fd = BZ2File(index_tab, 'r')
      else:
        tab_fd = open(index_tab, 'r')
    except:
      print 'no index table file present! - bye.'
      return

    table = Table(lbl_fd, tab_fd)
    # find index table in IndexProduct table, add if it does not exist
    table_products = IndexProduct.objects.filter(resource_url=index_table_id)
    if len(table_products) == 0:
      print 'create a new index table product', index_table_id
      table_product = IndexProduct(resource_url=index_table_id)
      table_product.column_names = table.column_names
      table_product.save()
    else:
      table_product = table_products[0]
      print 'found index table product', table_product.pk

    # set quick access model fields:
    self.set_model_fields()

    counter = 0
    batch_counter = 0
    counter_new = 0
    try:
      self.now = datetime.now()

      # iterate over table
      if offset > 0:
        for entry in table:
          counter = counter + 1
          if counter >= offset:
            break

      counter = 0
      for entry in table:
        try:
          product_id = entry.product_id
          value_list = []
          existing = self.get_query_set().filter(product_id=product_id)

          if len(existing) > 0 and force:
            batch_counter = batch_counter + 1
            value_list.append((self.pds_index_row_field, entry.value_list))
            value_list.append((self.pds_index_table_field, table_product.pk))
            value_list.append((self.modtime_field, self.now))
            update_values = [(f, None, f.get_db_prep_save(val)) for (f, val) in value_list]
            self.get_query_set().filter(pk=existing[0].pk)._update(update_values)
          elif len(existing) == 0:
            counter_new = counter_new + 1
            batch_counter = batch_counter + 1
            self.primer_from_index_row(entry, value_list)
            value_list.append((self.pds_index_row_field, entry.value_list))
            value_list.append((self.pds_index_table_field, table_product.pk))
            value_list.append((self.modtime_field, self.now))
            value_list.append((self.createtime_field, self.now))
            value_list.append((self.status_field, 'primer'))
            insert_values = [(f, f.get_db_prep_save(val)) for (f, val) in value_list]
            # values is a list of (field, field value) pairs
            self._insert(insert_values, return_id=True)
        except Exception, e:
          print 'error', repr(e)
          continue
        finally:
          counter = counter + 1
          # commit to DB a batch of 100 insert records
          if (batch_counter >= batchsize):
            transaction.commit()
            self.now = datetime.now() # update now statement for every batch submission
            print "committed batch of size %d from position %d, added new %d" % (batch_counter, counter, counter_new)
            batch_counter = 0
          if (counter % 500) == 0:
            print "processed %d documents" % counter
          if (limit is not None) and (counter >= limit):
            break

    finally:
      # before exiting commit the last batch to DB,
      # is this necessary with the decorator?
      if batch_counter > 0:
        print "committed remaining batch of size %d" % (batch_counter)
      transaction.commit()
      print "finished! Processed %d documents" % counter
      lbl_fd.close()
      tab_fd.close()
