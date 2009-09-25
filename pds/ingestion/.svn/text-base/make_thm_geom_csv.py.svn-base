import sys
from bz2 import BZ2File
from cum_index import Table

if __name__ == "__main__":

  try:
    bzfile = BZ2File('mgsGeo.csv.bz2', 'wU', 4096*4)
  except IOError:
    print "unable to create bz2 file!"
    sys.exit()

  label_file = 'THMIDX_IR.LBL'
  table_file = 'THMIDX_IR.TAB'

  table = Table(label_file, table_file)
  for entry in table:
    product_id = entry.observation_id
    try:
      ul = (entry.upper_left_longitude, entry.upper_left_latitude)
      ll = (entry.lower_left_longitude, entry.lower_left_latitude)
      ur = (entry.upper_right_longitude, entry.upper_right_latitude)
      lr = (entry.lower_right_longitude, entry.lower_right_latitude)
      phase_angle = entry.phase_angle
      incidence_angle = entry.incidence_angle
      emission_angle = entry.emission_angle
      # what about centerpixelresolution?

      bzfile.write('%s,%.10f,%.10f,%.10f,%.10f,%.10f,%.10f,%.10f,%.10f,%.10f,%.10f,%.10f\n' \
        %(product_id,ul[0],ul[1],ll[0],ll[1],lr[0],lr[1],ur[0],ur[1],phase_angle,incidence_angle,emission_angle) )
    except:
      print product_id
      continue

  bzfile.close()

  sys.exit()
