#!/usr/bin/env python

import os
import util
import math

_srtmbase = '/data/earth/srtm/'
_srtmfile = 'Z_%i_%i.TIF'
_egm96path = '/data/earth/egm96/egm96.tif'
_tile_size = 6000

class Earth(object):
    def __init__(self):

        print "Importing SRTM tiles and EGM96 data...."

        if not os.path.exists(_srtmbase) or not os.path.exists(_egm96path):
            print "Warning: SRTM or EGM96 data not found.  geo.earth.Earth will not function properly!"
            return

        import vw
        composite = vw.composite.ImageComposite(ptype=vw.int16)
        composite.set_draft_mode(True)

        for x in range(0,72):
            for y in range(0,24):
                path = _srtmbase + _srtmfile % (x+1,y+1)
                if os.path.exists(path):
                    v = vw.DiskImageView(path, ptype=vw.int16)
                    composite.insert(v, x*_tile_size, y*_tile_size)

        composite.prepare()
        self._srtm = composite.ref()

        # The vw Python bindings are buggy as hell.  Here we keep the DiskImageView 
        # objects to avoid a mysterious segfault, but we also keep the reference 
        # objects so we can index by pixel location.
        self._egm96_view = vw.DiskImageView(_egm96path, ptype=vw.float32)
        self._egm96 = self._egm96_view.ref()

        self._wgs84 = vw.cartography.Datum()

    def altitude(self, lat, lon):
        return util.interpolate_lat_lon(self._srtm, lat, lon, 60)

    def radius(self, lat, lon):
        alt = util.interpolate_lat_lon(self._srtm, lat, lon, 60)
        alt = alt + util.interpolate_lat_lon(self._egm96, lat, lon)
        xyz = self._wgs84.geodetic_to_cartesian((lon,lat,alt))
        #return xyz
        return math.sqrt(xyz[0]*xyz[0]+xyz[1]*xyz[1]+xyz[2]*xyz[2])
