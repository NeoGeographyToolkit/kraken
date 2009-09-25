#!/usr/bin/env python2.5

from time import strftime, strptime

from Cheetah.Template import Template
import spatial

# global variables'
global outDir
outDir = ''
global outFeed
outFeed = 'solr_feed.xml.gz'
global spatialFeatures
spatialFeatures = spatial.SpatialFeatures('/Users/kuehnel/NASA/Projects/PDS-Google/DDD-PDSTools/pds-tools/mars')

"""
This program scans a list of YAML files in a TAR archive
and produces the XML feed for Apache Solr as the output file.

Usage:
yaml2feed.py -o my_solr_feed.xml.gz -p '*.yaml' ~/tmp/yml_cl50.tgz

"""

# template stubs for creating feeds
partialImageTemplateStub = """\
  <doc boost="$boost">
    <field name="doc_boost">$boost</field>
    <field name="id">$urlresource.id</field>
#for $f in $tags
    <field name="$f[0]">$f[1]</field>
#end for
    <field name="cachebaseURL">${urlresource.cbase}</field>
    <field name="raw_format">8bit</field>
#for $g in $geotags
    <field name="geotags_$g[0]">$g[1]</field>
#end for
  </doc>
"""

from time import strftime, strptime

# perhaps group the filter keys into topic categories
mappingkeys = {'data_set_id':'data_set_id', 'target_name':'target_name',
  'mission_name':'mission_name', 'mission_phase_name':'mission_phase_name',
  'instrument_host_name':'instrument_host_name', 'instrument_id':'instrument_id',
  'product_id':'product_id', 'producer_id':'producer_id',
  'instrument_name':'instrument_name',
  'rationale_desc':'rationale_desc', 'orbit_number':'orbit_number',
  'product_release_date':'product_release_date', 'product_type':'product_type',
  'note':'note', 'source_image_id':'source_image_id', 'image_id':'product_id',
  'producer_institution_name':'producer_institution_name',
  'emission_angle':'centeremissionangle',
  'incidence_angle':'centerincidenceangle',
  'phase_angle':'centerphaseangle',
  'geometry_source_image_id':'source_image_id',
  'spacecraft_name':'spacecraft_name'}

product_times = {2001:"1991-09-23",2002:"1991-09-23",2003:"1991-09-23",2004:"1991-09-23",
 2005:"1991-09-23",2006:"1991-09-23",2007:"1992-05-13",2008:"1992-09-30",2009:"1992-09-30",2010:"1992-09-30",
 2011:"1992-09-30",2012:"1992-09-30",2013:"1992-09-30",2014:"1992-09-30",2015:"1999-03-01",2016:"1999-03-01",
 2017:"1999-03-01",2018:"1999-03-01",2019:"1999-03-01",2020:"1999-03-01",2021:"1999-03-01",2022:"1999-03-01" }

import yaml
import re

cleanup = re.compile('[\x00-\x1F]')

# extract URLs from data
def extractUrl(arrayOfhashes):

  hash = arrayOfhashes[0]
  list = hash['resourceURL']
  list.append(hash['image_id'])
  rURLs = [list]
  try:
    for h in arrayOfhashes[1:]:
      list = h['resourceURL']
      list.append(h['image_id'])
      rURLs.append(list)
  except IndexError:
    pass

  # no complaints  
  return rURLs

# condense a Solr feed from YAML data
def generateFeed(hash, xmlFile=None, volume=None, cachebase='', entity_id=''):

  resourceURLs = hash.get('resourceURLs', None)
  resourceURL = hash.get('resourceURL', None)
  urlIdentifier = ''

  #print 'generate feed for', entity_id

  if resourceURL is not None and resourceURLs is None:
    resourceURLs = [resourceURL]

  if resourceURLs is not None:
    for resourceURL in resourceURLs:
      rURL = resourceURL[0]
      try:
        resourceURL[2]
      except IndexError:
        resourceURL.append('')
      fileExt = rURL[-3:].upper()
      if 'IMG'.endswith(fileExt):
        resourceURL[2] = 'IMG'
      elif 'IMQ'.endswith(fileExt):
        resourceURL[2] = 'IMQ'
      elif 'LAB'.endswith(fileExt):
        resourceURL[2] = 'LAB'
      elif 'LBL'.endswith(fileExt):
        resourceURL[2] = 'LBL'

  if True: #fileExt in ['IMG', 'IMQ', 'QUB', 'CUB']: # found the image entry
    urlpathList = rURL.lower().split('/')[::-1]
    for component in urlpathList:
      if ((component in ['cdroms']) or (component[-4:] == '.gov') or (component[-4:] == 'data')):
        break;
      else:
        urlIdentifier = component + '/' + urlIdentifier

  # remove last path separator
  urlIdentifier = urlIdentifier.rstrip('/')
  #print urlIdentifier

  # assemble image or image map tags
  tags = []
  try:
    # gather information about the image dimensions
    try:
      image_data = hash['image'] # try a cube structure if not an image
      tags.append(['image_width', image_data['line_samples']])
      tags.append(['image_height', image_data['lines']])
      width = int(image_data['line_samples'])
      height = int(image_data['lines'])
      if width < height:
        aspect_ratio = float(width)/float(height)
      else:
        aspect_ratio = float(height)/float(width)
      tags.append(['aspect_ratio', aspect_ratio])
      try:
        tags.append(['bands', image_data['bands']])
      except KeyError:
        tags.append(['bands', '1'])
    except KeyError:
      try:
        image_data = hash['qube'] # if not a qube then throw an exception, don't process further
      except KeyError:
        image_data = hash['spectral_qube']
      dimensions = image_data['core_items']
      tags.append(['aspect_ratio', float(dimensions[0])/float(dimensions[1])])
      tags.append(['image_width', dimensions[0]])
      tags.append(['image_height', dimensions[1]])
      
      try:
        tags.append(['bands', dimensions[2]])
      except IndexError:
        tags.append(['bands', '1'])

    # gather information about the image map projection if available
    map_projection = None
    try:
      map_projection = hash['image_map_projection_catalog']
      # test for inconsistencies:
      if map_projection['map_projection_type'].lower() in ['none', 'null']:
        raise KeyError
    except KeyError:
      if hash.has_key('image_map_projection'):
        map_projection = hash['image_map_projection']
      elif (hash.has_key('qube') and hash['qube'].has_key('image_map_projection')):
        map_projection = hash['qube']['image_map_projection']
      else:
        map_projection = None

    mscale = None
    if map_projection:
      try:
        try:
          map_scale = map_projection['map_scale'] # map scale is in KM/M per pixel
          try:
            mscale = float(map_scale[0])
            if map_scale[1].startswith('<KM'):
              mscale = mscale*1000
          except:
            mscale = float(map_scale)*1000 # assume value in in KM per pixel
          tags.append(['map_scale', mscale])
        except:
          pass

        try:
          map_resolution = map_projection['map_resolution'] # map resolution is in DEG per pixel
          try:
            mres = float(map_resolution[0])
          except:
            mres = float(map_resolution)
          tags.append(['map_resolution', mres])
        except:
          pass

        tags.append(['map_projection', map_projection['map_projection_type'].upper()])
        top = float(map_projection['maximum_latitude'])
        bottom = float(map_projection['minimum_latitude'])
        tags.append(['max_lat', map_projection['maximum_latitude']])
        tags.append(['min_lat', map_projection['minimum_latitude']])
        if map_projection['positive_longitude_direction'].lower() == 'east':
          east = float(map_projection['easternmost_longitude'])
          west = float(map_projection['westernmost_longitude'])
          tags.append(['west_lon', map_projection['westernmost_longitude']])
          tags.append(['east_lon', map_projection['easternmost_longitude']])
        else:
          west = 360.0-float(map_projection['maximum_longitude'])
          east = 360.0-float(map_projection['minimum_longitude'])
          #print 'positive longitude direction is westward!', east, west
          tags.append(['west_lon', '%.7f'%(west)])
          tags.append(['east_lon', '%.7f'%(east)])          

        bbox = (west, bottom, east, top)
      except: # something went wrong with
        print 'something went wrong with the map parameters'
        map_projection = None

    # process time tags
    if hash.has_key('product_creation_time'):
      timetag = strptime(hash['product_creation_time'], "%Y-%m-%dT%H:%M:%S")
      tags.append(['product_creation_time', strftime("%Y-%m-%dT%H:%M:%SZ", timetag)])

    try:
      if hash.has_key('image_time'):
        timetag =  strptime(hash['image_time'], "%Y-%m-%dT%H:%M:%SZ")
        tags.append(['image_time', strftime("%Y-%m-%dT%H:%M:%SZ", timetag)])
    except:
      pass

    # process time tag, i.e. 1997-06-18T14:39:24
    if not hash.has_key('product_release_date'):
      timetag = strptime(product_times[int(volume)], "%Y-%m-%d")
      tags.append(['product_release_date', strftime("%Y-%m-%dT%H:%M:%SZ", timetag)])

    entity_type = None
    keys = hash.keys()
    filterkeys = mappingkeys.keys()
    fkeys = [k for k in keys if k in filterkeys]
    for key in fkeys:
      obj = hash[key]
      mkey = mappingkeys[key]
      if mkey == 'data_set_id':
        obj = obj.replace('V01/V02','VO1/VO2') # correct mistake
      if mkey == 'note':
        obj = cleanup.sub('',obj)
        if obj == 'PHASE ANGLE':
          entity_type = 'image map phase-angle'
      if isinstance(obj, list): # iterate over list
        for elem in obj:
          if isinstance(elem, basestring):
            clean_elem = elem.strip(' ').rstrip(' ') # clean up strings
            if len(clean_elem) == 0:
              continue
            else:
              tags.append([mkey, clean_elem])
          else:
            tags.append([mkey, elem])
      else:
        if isinstance(obj, basestring):
          clean_obj = obj.strip(' ').rstrip(' ').replace('\r\n','')
          tags.append([mkey, clean_obj])
        else:
          tags.append([mkey, obj])

    # add filter information if available
    if hash.has_key('center_filter_wavelength'):
      wavelength = hash['center_filter_wavelength']
      tags.append(['filter', '%s' % wavelength])
      if wavelength == 750.0:
        min_wavelength = '400'
        max_wavelength = '800'
      else:
        min_wavelength = min(wavelength) #min_wavelength = '395'
        max_wavelength = max(wavelength) #max_wavelength = '1015'
      tags.append(['min_filter', '%s' % min_wavelength])
      tags.append(['max_filter', '%s' % max_wavelength])
    elif hash.has_key('qube'):
      try:
        bins = hash['qube']['band_bin']
        wavelength = bins['band_bin_center']
        # convert wavelength to nm if necessary
        if isinstance(wavelength,list):
          wavelengthstring = '[%s]' % (','.join(['%.0f'%(num*1000) for num in wavelength])) # conversion from micrometer to nanameter
          tags.append(['filter', wavelengthstring])
          tags.append(['min_filter', '%.0f' % (min(wavelength)*1000.0)])
          tags.append(['max_filter', '%.0f' % (max(wavelength)*1000.0)])
        else:
          tags.append(['filter', '%.0f' % wavelength])
      except:
        print 'no wavelength definition'
    else: # add manually filter information
      decode = entity_id.split('_')
      #print decode
      if 'red' in decode:
        filter = 'red'
        minwlen = 550
        maxwlen = 700
      elif 'grn' in decode:
        filter = 'green'
        minwlen = 500
        maxwlen = 600
      elif 'vio' in decode:
        filter = 'violett'
        minwlen = 350
        maxwlen = 470
      elif 'sgr' in decode:
        filter = 'synthetic green'
        minwlen = 350
        maxwlen = 700
      else:
        filter = None
      
      if filter is not None:
        tags.append(['filter', filter])
        tags.append(['min_filter', '%.0f' % (minwlen)])
        tags.append(['max_filter', '%.0f' % (maxwlen)])
    
    for rURL in resourceURLs:
      if rURL[2] in ['IMG','IMQ']:
        tags.append(['resourceURL_img', rURL])
      elif rURL[2] in ['LBL','LAB']:
        tags.append(['resourceURL_lbl', rURL])

    # disect product name for partial searches
    product_id = hash['image_id']
    #p1, p2, p3 = product_id[:2], product_id[2:5], product_id[5:]
    #p1, p2, p3 = product_id[:1], product_id[1:5], product_id[5:]

    #tags.append(['content', '%s %s %s %s%s %s%s'%(p1,p2,p3,p1,p2,p2,p3) ])

    # identify object as an image
    if map_projection:
      if entity_type is None:
        tags.append(['entity_type','image map'])
      else:
        tags.append(['entity_type',entity_type])
    else:
      tags.append(['entity_type', 'image'])

    tags.append(['entity_id', entity_id])

    geotags = None
    if map_projection:
      #print bbox
      geotags = spatialFeatures.getFeatures(bbox)

    #title = hash['image_id'].replace('/','_').replace('-','_').replace('.','_') # clean up title characters
    #description = tdesc + '\n' + hash.get('description', '') + '\n' + image_data.get('description', '') + '\n'
  except KeyError:
    return None

  # come up with a fictional http address, target, mission, instrument
  (base,sep,ext) = urlIdentifier.rpartition('.')
  urlIdentifier = '.'.join([base,'img'])
  httpurl = 'http://pds.nasa.gov/image/' + urlIdentifier

  if geotags is None:
    geotags = []

  if mscale is not None:
    boost = aspect_ratio/mscale
  else:
    boost = 'NaN'

  print '/'.join([cachebase,entity_id])
  if xmlFile:
    t = Template(partialImageTemplateStub,
      searchList={'boost':boost,
      'urlresource':{'id':httpurl,'cbase':'/'.join([cachebase,entity_id]).lower()},
      'tags':tags,'geotags':geotags})
    xmlFile.write(str(t))

  # no complaints  
  return True

import tarfile
from gzip import GzipFile
import sys, os
import getopt

class Usage(Exception):
  def __init__(self, msg):
    self.msg = msg

def main(argv=None):
  global outDir, outFeed

  if argv is None:
    argv = sys.argv
  # parse command line options
  try:
    try:
      opts, args = getopt.getopt(argv[1:], 'ho:p:', ['help', 'outfile=','pattern='])
    except getopt.error, msg:
      raise Usage(msg)
  except Usage, err:
    print>>sys.stderr, err.msg
    print>>sys.stderr, "for help use --help"
    return

  # process options
  filelist = None
  filepattern = '*.yaml'
  for o, a in opts:
    if o in ('-h', '--help'):
      print __doc__
      return
    elif o in ('-o', '--outfile'):
      outFeed = a
    elif o in ('-p', '--pattern'):
      filepattern = a.strip('/')

  try:
    yaml_repository = args[0].rstrip('/')
    yrepos = yaml_repository.lower()
  except IndexError:
    print __doc__
    return

  # test if is a TAR repository
  if yrepos.endswith('.tar') or yrepos.endswith('.tgz') or yrepos.endswith('.tar.gz'):
    try:
      tarrepos = tarfile.open(yaml_repository,'r')
    except tarfile.ReadError:
      tarrepos = tarfile.open(yaml_repository,'r',GzipFile(yaml_repository,'r'))

    member = tarrepos.next()
    try:
      xmlFile = GzipFile(outFeed,'w')
    except IOError, e:
      print "cannot open output file '%s'" %outFeed
      raise

    if member:
        xmlFile.write('<?xml version="1.0" encoding="UTF8"?>\n')
        xmlFile.write('<add xmlns:string="java:java.lang.String">\n')
    while member:
      if member.isreg() and member.name.endswith('.yaml'):
        try:
          pathelements = member.name.split('/')
          basename = (pathelements[-1].partition('.'))[0]
          arrayOfhashes = yaml.load(tarrepos.extractfile(member).read())
          merge = False # merge labels and data sets
          urls = extractUrl(arrayOfhashes)
          cachebase = '/'.join(['vo',pathelements[1]])
          if len(urls) == 1:
            entity_id = basename
            generateFeed(arrayOfhashes[0], xmlFile, \
              volume=pathelements[1], cachebase=cachebase, entity_id=entity_id)
          elif len(urls) > 1 and merge:
            entity_id = basename
            superHash = {}
            rURLs = []
            for idx in range(len(urls)):
              anotherHash = arrayOfhashes[idx]
              for key in anotherHash:
                if key == 'resourceURL':
                  rURLs.append(anotherHash['resourceURL'])
                else:
                  # superHash.update(morehash) -> don't use this, it overwrites the existing key,value pairs
                  superHash.setdefault(key, anotherHash[key])
                superHash.setdefault('resourceURLs', rURLs)
            generateFeed(superHash, xmlFile, \
              volume=pathelements[0], cachebase=cachebase, entity_id=entity_id)
            #break # for test purpose
          elif len(urls) > 1 and not merge:
            for idx in range(len(urls)):
              url = urls[idx]
              urlelements = url[0].split('/')
              entity_id = url[2].lower().replace('-','_')
              entity_elements = entity_id.split('_')
              if urlelements[-2].startswith(entity_elements[-1]):
                entity_elements[-1] = urlelements[-2]
                entity_id = '_'.join(entity_elements)
              elif not entity_id.endswith(urlelements[-2]):
                entity_id = '_'.join([entity,urlelements[-2]])
              #print arrayOfhashes[idx]
              generateFeed(arrayOfhashes[idx], xmlFile, \
                volume=pathelements[1], cachebase=cachebase, entity_id=entity_id)
          else:
            continue
        except:
          print 'problem with', member.name
          #print arrayOfhashes

      member = tarrepos.next()
    if xmlFile:
      xmlFile.write('</add>\n')
      xmlFile.close()
    return

if __name__ == "__main__":
  sys.exit(main())