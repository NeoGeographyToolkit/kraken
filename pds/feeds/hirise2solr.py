import yaml
from bz2 import BZ2File
from datetime import datetime

from base_solr import SolrFeedBurner
from pds.models import Product

class HiriseFeed(SolrFeedBurner):

  identifier_prefix = 'MRO-M-HIRISE-3-RDR-V1.0'.lower()
  previewURL_prefix = 'http://byss.arc.nasa.gov/mdh/copernicus/hirise/thumbs'

  def __init__(self, xmlfile='test'):
    super(HiriseFeed, self).__init__(xmlfile, 'mars')

  def getResourceURL(self, csv_file='urls_csv.bz2'):
    try:
      csv_file = BZ2File(csv_file,'w')
    except IOError, e:
      print 'csv file, %s, cannot be opened!' % csv_file
      raise

    try:
      # return a list of hashed values, no python objects!
      qs = Product.hirise_objects.all().values('product_id', 'resource_url')
      offset = 0
      limit = 1000
      results=qs[offset:offset+limit]
      while len(results) > 0:
        for entry in results:
          csv_file.write('%s,%s\n' %(entry['resource_url'], entry['product_id']))

        print 'batch', offset, limit
        offset = offset + limit
        results=qs[offset:offset+limit]
    finally:
      if csv_file:
        csv_file.close()

  def generateFeed(self):
    # query set lazy execution
    try:
      self.start()
      qs = Product.hirise_objects.all()
      offset = 0
      limit = 1000
      results=qs[offset:offset+limit]
      while len(results) > 0:
        for entry in results:
          tags = []

          try:
            rURL = entry.resource_url.lower()
            if rURL.endswith('lbl'):
              tags.append(('resourceURL_lbl', entry.resource_url))
              identifier_partial = '/'.join(rURL[:-4].split('/')[4:])
              url_prefix_components = entry.resource_url.split('/')[:4]
            elif rURL.endswith('img'):
              tags.append(('resourceURL_img', rURL))

            # add result
            metadata = yaml.load(entry.metadata) # TODO use faster serialization, i.e. cPickle

            url_prefix_components.append(metadata['file_name_specification'])
            tags.append(('resourceURL_img', ['/'.join(url_prefix_components), 5602739058, 'JP2']))
            tags.append(('resourceBYTES_img', 5602739058))
            tags.append(('previewURL', "%s/%s.jpg" % (self.previewURL_prefix,entry.observation_id) ))

            tags.append(('image_width', metadata['line_samples']))
            tags.append(('image_height', metadata['image_lines']))
            try:
              aspect_ratio = float(metadata['line_samples'])/float(metadata['image_lines'])
              tags.append(('aspect_ratio', aspect_ratio))
            except:
              pass
            tags.append(('bands', 1))

            tags.append(('centeremissionangle', metadata['emission_angle']))
            tags.append(('centerincidenceangle', metadata['incidence_angle']))
            tags.append(('centerphaseangle', metadata['phase_angle']))
            #tags.append(('centerpixelresolution', metadata['centerpixelresolution']))

            # map related properties
            tags.append(('map_projection', metadata['map_projection_type']))
            tags.append(('map_resolution', metadata['map_resolution']))
            tags.append(('map_scale', metadata['map_scale']))
            tags.append(('max_lat', metadata['maximum_latitude']))
            tags.append(('min_lat', metadata['minimum_latitude']))
            tags.append(('west_lon', metadata['minimum_longitude'])) # ??
            tags.append(('east_lon', metadata['maximum_longitude'])) # ??

            tags.append(('entity_type', 'image map'))
            tags.append(('entity_id', metadata['product_id'].lower()))

            # product PDS metadata
            tags.append(('rationale_desc', metadata['rationale_desc']))
            tags.append(('mission_phase_name', metadata['mission_phase_name']))
            tags.append(('instrument_host_id', metadata['instrument_host_id']))
            tags.append(('spacecraft_name', 'MARS RECONNAISSANCE ORBITER'))
            tags.append(('instrument_name', 'HIGH RESOLUTION IMAGING SCIENCE EXPERIMENT'))
            tags.append(('orbit_number', metadata['orbit_number']))

            entry.dataset_id = 'MRO-M-HIRISE-3-RDR-V1.0' # metadata in field is wrong

            # origin
            tags.append(('producer_id', 'UA'))
            tags.append(('producer_institution_name', 'UNIVERSITY OF ARIZONA'))

            # time tags
            #tags.append(('product_creation_time', ))
            tags.append(('image_time', metadata['start_time'].strftime("%Y-%m-%dT%H:%M:%SZ") ))
            self.add_document(tags, entry, self.identifier_prefix+'/'+identifier_partial)

          except Exception, e:
            print 'error:', repr(e)
            continue

        print 'batch', offset, limit
        offset = offset + limit
        results=qs[offset:offset+limit]
        # only for experimentation
        #break

    finally:
        # clean up
        self.finish()

# example
# from feeds.hirise2solr import HiriseFeed
# f = HiriseFeed()
# f.getResourceURL()