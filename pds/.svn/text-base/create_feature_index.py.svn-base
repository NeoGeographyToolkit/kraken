# pickle is fasted option for serialization
import pickle
import os
import sys
import math
from bz2 import BZ2File

# prepend library search path
sys.path.insert(0, os.path.abspath('./lib'))

from rtree import Rtree

body_radius = {'moon':1737.4, 'mars':3389.5}

# create spatial index in file
def create_index(csv_fd, separator, target):

  target_index = Rtree(target, pagesize=64) # filter out the moon (default pagesize is 4096)
  target_features = []
  try:
    radius = body_radius[target]
  except KeyError:
    return

  # process input data, skip the first two lines
  csv_fd.readline()
  csv_fd.readline()

  for line in csv_fd: # use xreadlines for big files
    data = line.split(separator)
    try:
      body_system = data[0][1:-1] # strip quotes from string
      body = data[1][1:-1]        # strip quotes from string
      approval = data[15][1:-1].split(',')
      if 'Dropped' in approval:
        continue

      if not (body_system == 'Mars' and body[0:5] == 'MarsE') and not (body_system == 'Earth' and body[0:4] == 'Moon'):
        continue

      bodyshort = body[0:4].lower()
      if bodyshort != target:
        continue

      feature_idx = len(target_features)
      feature_name = unicode(data[2].lstrip('" [').rstrip('] "')).encode('utf-8')

      try:
        projDir = data[9].rstrip('"').strip('"')
      except:
        projDir = 'E'  # east is positive

      (lat, lon) = (float(data[3]), float(data[4])) # lon [0,360] and lat [-90,90]
      if projDir == 'W':
        lon = 360.0 - lon

      target_index.add(feature_idx, (lon, lat))

      try:
        diameter = float(data[10])
      except ValueError:
        diameter = 0.0

      try:
        [south, north, west, east] = [float(x) for x in data[5:9]]

        if projDir == 'W':
          west = 360.0 - west
          east = 360.0 - east

        target_index.add(feature_idx, (west, south, east, north))

        # some test for debugging
        #if feature_name[:5] == 'Gusev':
        #  print body, feature_name, projDir, feature_index, (west,south,east,north)

      except (IndexError, ValueError):
        # no bounding box available?
        deltaLatH = math.atan(diameter/radius)*90.0/math.pi # spherical moon radius 1737.4km
        # deltaLat is also deltaLon at the equator
        deltaLonH = math.atan(diameter/(radius*math.cos(lat*math.pi/180.0)))*90.0/math.pi

        # some test for debugging
        #if feature_name[:5] == 'Gusev':
        #  print (2.0*deltaLatH, 2.0*deltaLonH), 'bounding box for', data[2][1:-1], diameter, 'at lat/lon', (lat,lon)

        (south, north) = (lat - deltaLatH, lat + deltaLatH)
        if south > -90 and north < 90:
          (west, east) = (lon - deltaLonH, lon + deltaLonH)
        else:
          (west, east) = (0, 360)

        target_index.add(feature_idx, (west, south, east, north))

      # add the geographic feature name and type to the index
      ftype = data[18][1:-1]
      if len(ftype.split(' ')) > 2: # something is wrong ?
        ftype = unicode(data[-2][1:-1])

      feature_type = unicode(ftype).encode('utf-8')
      if len(ftype.split(' ')) > 2:
        continue

      # feature type, feature name strip quotes from string
      target_features.append((ftype, feature_name, diameter, (west,south,east,north)))

      # some test for debugging
      #if feature_name[:5] == 'Gusev':
      #  print body, (ftype, feature_name, diameter, (west,south,east,north))

    except (IndexError, ValueError):
      continue

  # print statistics
  sys.stdout.write("%d features found for %s\n" %(len(target_features), target) )
  # pickle is faster than yaml
  pklfile = open("%s.pkl" % target, 'wb')
  pickle.dump(target_features, pklfile)
  pklfile.close()

  # run some tests
  hits = [n for n in target_index.intersection((359.94720086454998, -89.745498818173004, 360.0, 83.234587578128995))]
  print "%d hits in search box" % len(hits)

if __name__ == "__main__":

  target = 'mars' # currently only works for moon, mars
  csv_file = 'AllDataDelimited032409.txt.bz2'
  # open USGS named geo-feature CSV file
  try:
    csv_fd = BZ2File(csv_file, 'r')

    # remove previous rtree index files 
    sys.stdout.write('Remove old spatial index.\n')
    try:
      os.remove("%s.dat" % target)
    except OSError:
      pass
    try:
      os.remove("%s.idx" % target)
    except OSError:
      pass
    try:
      os.remove("%s.pkl" % target)
    except OSError:
      pass

    sys.stdout.write("Create new spatial index for %s.\n" % target)
    create_index(csv_fd, ':', target)

    csv_fd.close()

  except IOError, e:
    sys.stderr.write('cannot open data file!')
