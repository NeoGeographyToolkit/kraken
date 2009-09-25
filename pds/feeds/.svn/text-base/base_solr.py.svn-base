try:
  import cPickle as pickle
except ImportError:
  import pickle
import gzip
try:
  from cStringIO import StringIO
except ImportError:
  import StringIO
import math
import sys
import platform

deg2rad = math.pi/180.0
body_radius = {'moon':1737.4, 'mars':3396.19}

# used for geo-spatial tagging
rtree_module = False
if platform.system() == 'Java'
  pass
else:
  from rtree import Rtree
  rtree_module = True

# for explicit Solr XML generation via Django templates
from django.template import Context, Template

class SolrFeedBurner(object):

  # template stubs for creating solr feed
  addDoc = Template("""{% load gapless_tag %}\
  <doc boost="{{ boost }}">
    <field name="doc_boost">{{ boost }}</field>
    <field name="id">{{ unique_id }}</field>{% gapless %}
{% for ftag in ftags %}
    <field name="{{ftag.0}}">{{ftag.1}}</field>
{% endfor %}
{% for geotag in geotags %}
    <field name="geotags_{{geotag.0}}">{{geotag.1}}</field>
{% endfor %}
  </doc>{% endgapless %}
""")

# need to take care of those fields manually
#  <field name="cachebaseURL">{{ urlresource.cbase }}</field>
#  <field name="raw_format">8bit</field>

  class GeoExtent(object):

    # geographic extent in longitude/latidute SRID 4326
    def __init__(self, west, south, east, north):
      self.west = west
      self.east = east
      self.north = north
      self.south = south

    def __str__(self):
      return "{west:%s, south:%s, east:%s, north:%s}" % (self.west, self.south, self.east, self.north)

    def is_crossing_meridian(self):
      return bool(self.west > self.east)

    def bbox(self):
      return [self.west, self.south, self.east, self.north]
    def bbox_part1(self):
      return [self.west, self.south, 360.0, self.north]
    def bbox_part2(self):
      return [0.0, self.south, self.east, self.north]

    def solid_angle(self):
      return (self.east-self.west)*deg2rad*(math.sin(self.north*deg2rad) - math.sin(self.south*deg2rad))

    def find_overlap(self, fbbox):
      # calculate lat/lon box of overlap area
      minlon = max(self.west, fbbox[0])
      maxlon = min(self.east, fbbox[2])
      minlat = max(self.south, fbbox[1])
      maxlat = min(self.north, fbbox[3])
      # print 'overlap bbox', (minlon,minlat,maxlon,maxlat)
      # overlap area in solid angle
      return (maxlon-minlon)*deg2rad*(math.sin(maxlat*deg2rad) - math.sin(minlat*deg2rad))

  def __init__(self, xmlfile_base, target='mars'):
    self.xmlfile_base = xmlfile_base
    # load spatial index file for the proper target
    try:
      self.target_radius = body_radius.get(target, 1.0)
      self.sindex = Rtree(target) if rtree_module else None
      pklfile = open("%s.pkl" % target, 'r')
      self.sfeatures = pickle.load(pklfile)
      pklfile.close()      
    except Execption:      
      self.sindex = None   
      self.sfeatures = None

  def start(self, file_suffix=''):
    try:
      if len(file_suffix) > 1:
        file_name = '%s_%s.xml.gz' % (self.xmlfile_base, file_suffix)
      else:
        file_name = self.xmlfile_base + '.xml.gz'

      xmlfile = gzip.open(file_name, 'wb')
      xmlfile.write('<?xml version="1.0" encoding="UTF8"?>\n')
      xmlfile.write('<add xmlns:string="java:java.lang.String">\n')
      self.xmlSolr = xmlfile
    except IOError, e:
      self.xmlSolr = None
      raise Exception, "cannot open output file '%s'" % xmlfile

  def finish(self):
    if self.xmlSolr:
      self.xmlSolr.write('</add>\n')
      self.xmlSolr.close()

  def add_document(self, tags=[], entry=None, id_suffix=''):
    # add common tags:
    if entry:
      tags.append(('product_id', entry.product_id))
      tags.append(('instrument_id', entry.instrument_id))
      tags.append(('data_set_id', entry.dataset_id))
      tags.append(('observation_id', entry.observation_id))
      tags.append(('target_name', entry.target_name))
    # TODO:
    # Determine boost factor from centerpixel resolution, image aspect ratio...

    footprint = getattr(entry, 'footprint', None)
    gtags = []
    # geographic computations, SRID 4326 -> geographic coordinates with (-180, 180] east positive latitudes
    if footprint:
      # convert into solr corner_footprint format
      #[-26.29727498 25.03398274 -25.95621346 24.98516899 -25.95918670 24.92862858 -26.29997775 24.97726311]"
      if len(footprint.exterior_ring) == 5:
        corner_footprint = StringIO()
        corner_footprint.write('[')
        for idx in range(0,5):
          # corner footprint needs longitude latitude tuples with 0-360 positive east degrees of longitude
          lon, lat = footprint.exterior_ring[idx]
          lon = lon if lon >= 0.0 else lon + 360.0
          corner_footprint.write("%s %s " % (lat, lon) if idx < 4 else ']')

        tags.append(('corner_footprint', corner_footprint.getvalue()))
      # get lat/lon bounding box
      lon_west, lat_min, lon_east, lat_max = footprint.exterior_ring.extent
      lon_west = lon_west if lon_west >= 0.0 else lon_west + 360.0
      lon_east = lon_east if lon_east >= 0.0 else lon_east + 360.0
      # add bounding box if not already present:
      if len([tag for tag in tags if tag[0] == 'min_lat']) == 0:
        tags.append(('max_lat', lat_max))
        tags.append(('min_lat', lat_min))
        tags.append(('west_lon', lon_west))
        tags.append(('east_lon', lon_east))

      extent = self.GeoExtent(lon_west, lat_min, lon_east, lat_max)
      if self.sindex is not None:
        hits = self.get_spatial_features(extent)
        #print "found %d hits in extent %s" % (len(hits), str(extent))
        gtags = self.rank_spatial_features(extent, hits)

    # construct an unique product identifier, for lucene indexing db!
    unique_id = '/'.join(['http://pds.nasa.gov/image', id_suffix])
    # calculate boost factor
    factors = dict([tag for tag in tags if tag[0] == 'aspect_ratio' or tag[0] == 'centerpixelresoluiton'\
     or tag[0] == 'map_scale' or tag[0] == 'map_resolution'])      
    try:
      logar = math.log(float(factors['aspect_ratio']))
      boost = 1.0/(1.0 + logar*logar)
    except:
      boost = 1.0
    solr_doc = Context({'boost':boost, 'unique_id':unique_id, 'ftags':tags, 'geotags':gtags})
    self.xmlSolr.write(self.addDoc.render(solr_doc))

  def get_spatial_features(self, extent):
    if extent.is_crossing_meridian():
      print 'meridian crossing'
      result = self.sindex.intersection(extent.bbox_part1()) + \
               self.sindex.intersection(extent.bbox_part2())
    else:
      result = self.sindex.intersection(extent.bbox())

    hits = [self.sfeatures[n] for n in set(result)] # keep id unique
    #print len(hits)
    if len(hits) > 200: # take only the 200 most important ones
      hits.sort(cmp=lambda x,y: int(y[2]-x[2]))
      hits = hits[0:200]

    return hits

  def rank_spatial_features(self, extent, features):
    # ranked results sorted into size categories and feature_type categories
    ranking_hash = {}
    # calculate overlap of spatial feature with image area
    image_solid_angle = extent.solid_angle()
    #image_area = image_solid_angle*self.target_radius^2 (dimensions are km^2)

    for f in features:
      try:
        feature_type = f[0] # utl-8 encoded
        feature_name = f[1] # utf-8 encoded
        feature_radius = f[2]
        feature_bbox = f[3]

        # feature area in solid angle: (solid angle of a cone with angle theta)
        theta = (feature_radius/(2.0*self.target_radius)) # diameter/circumference * math.pi
        feature_solid_angle = 2.0*math.pi*(1.0-math.cos(theta))

        # overlap in solid angle
        overlap_solid_angle = extent.find_overlap(feature_bbox)

        # compute and classify feature visibility
        try:
          visibility = overlap_solid_angle/image_solid_angle * overlap_solid_angle/feature_solid_angle
          # 0 <= visibility <= 1, classify into four categories
          if visibility > 0.66:
            size_category = 'full'
          elif visibility > 0.33:
            size_category = 'partial'
          elif visibility > 0.1:
            size_category = 'minor'
          else:
            size_category = 'little'
        except Exception, e:
          print 'Classification Error', e, 'feature solid angle', feature_solid_angle, 'overlap solid angle', overlap_solid_angle
          size_category = 'unkown'

        ranking_hash.setdefault(size_category, {}).setdefault(feature_type, []).append(feature_name)

      except AttributeError:
        continue
      except Exception, e:
        sys.stderr.write("some error occured %s" % repr(e))
        continue

    # assemble result
    result = []
    for sk, v in ranking_hash.iteritems():
      for fk, v2 in v.iteritems():
        result.append((sk+'_'+fk.split(',',1)[0].lower(), fk+': '+','.join(v2)))
    #print result
    return result
