import yaml
import cPickle
from datetime import datetime

from base_solr import SolrFeedBurner
from pds.models import Product

from django.db import connection, transaction

class ThemisFeed(SolrFeedBurner):

  externalURL_prefix = 'http://themis-data.asu.edu/planetview/inst/themis'
  pdsURL_prefix = 'http://static.mars.asu.edu/pds/data'

  def __init__(self, xmlfile):
    super(ThemisFeed, self).__init__(xmlfile, 'mars')

  def generateFeed(self, start=0, num=10):
    # query set lazy execution
    try:
      self.start('%d_%d' % (start,num))
      qs = Product.ody_objects.all()
      offset = start
      limit = min(200, num)
      results=qs[offset:offset+limit]
      print connection.queries[-1]
      while len(results) > 0:
        for entry in results:
          tags = []

          try:
            # add result
            pdslabel = cPickle.loads(entry.pds_label.encode("utf-8"))
            metadata_array = yaml.load(entry.metadata)
            #print entry.metadata, entry.footprint
            metadata = metadata_array[0]
            #print metadata

            rURL = entry.resource_url.lower()
            resource = pdslabel['resourceURL']
            urlpath = resource[0]
            bytes = resource[1]
            # correct the problem with the static url
            url_components = urlpath.split('/')[-3:]
            url = '/'.join([self.pdsURL_prefix] + url_components)
            identifier_partial = '/'.join(url_components)
            tags.append(('resourceURL_img', [url, bytes, 'QUB']))
            tags.append(('resourceBYTES_img', bytes))

            # correct the observation id
            observation_id = entry.product_id[:-3]
            entry.observation_id = observation_id

            # image specs
            cubedata = pdslabel['spectral_qube']
            dimensions = cubedata['core_items']
            tags.append(('image_width', dimensions[0]))
            tags.append(('image_height', dimensions[1]))
            try:
              aspect_ratio = float(dimensions[0])/float(dimensions[1])
              tags.append(('aspect_ratio', aspect_ratio))
            except:
              pass
            tags.append(('bands', dimensions[2]))

            if cubedata['band_bin']['band_bin_unit'].lower() == 'micrometer':
              filter_array = map(lambda x: x*1000.0, cubedata['band_bin']['band_bin_center'])
            else:
              filter_array = cubedata['band_bin']['band_bin_center']

            tags.append(('filter', filter_array))
            tags.append(('min_filter', filter_array[0]))
            tags.append(('max_filter', filter_array[-1]))

            tags.append(('centeremissionangle', metadata['centeremissionangle']))
            tags.append(('centerincidenceangle', metadata['centerincidenceangle']))
            tags.append(('centerphaseangle', metadata['centerphaseangle']))
            #tags.append(('centerpixelresolution', metadata['centerpixelresolution']))

            tags.append(('entity_type', 'image'))
            entity_id = pdslabel['product_id'].lower()
            tags.append(('entity_id', entity_id))

            # product PDS metadata
            tags.append(('rationale_desc', cubedata['description']))
            tags.append(('mission_name', pdslabel['mission_name']))
            tags.append(('mission_phase_name', pdslabel['mission_phase_name']))
            tags.append(('spacecraft_name', pdslabel['instrument_host_name']))
            tags.append(('instrument_name', pdslabel['instrument_name']))
            tags.append(('orbit_number', pdslabel['orbit_number']))

            # origin
            tags.append(('producer_id', pdslabel['producer_id']))
            tags.append(('producer_institution_name', 'ARIZONA STATE UNIVERSITY'))

            # preview & links
            cache_url_components = ['ody']+url_components[:2]+[entity_id]
            tags.append(('cachebaseURL', '/'.join(cache_url_components)))
            tags.append(('raw_format', '16bit'))
            tags.append(('externalURL', ['/'.join([self.externalURL_prefix, observation_id]).encode('utf-8'), 'ASU'] ))

            # time tags
            tags.append(('product_creation_time',pdslabel['product_creation_time']+'Z')) #.strftime("%Y-%m-%dT%H:%M:%SZ") ))
            tags.append(('image_time',pdslabel['start_time']+'Z')) #.strftime("%Y-%m-%dT%H:%M:%SZ") ))
            self.add_document(tags, entry, entry.dataset_id+'/'+identifier_partial)

          except Exception, e:
            print 'error:', repr(e)
            continue

        print 'batch', offset, limit
        offset = offset + limit
        if offset < start+num:
          results=qs[offset:offset+limit]
        else:
          break

    finally:
        # clean up
        self.finish()
        
# example
# from feeds.themis2solr import ThemisFeed
# f = ThemisFeed('test')
# f.generateFeed()
