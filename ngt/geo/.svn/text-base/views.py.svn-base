import moon, mars, util

rpc_handler = util.XMLRPCHandler()

#_earth = earth.Earth()
_moon = moon.Moon()
_mars = mars.Mars()

#@rpc_handler.register('earth.altitude')
#def earth_altitude(pos, lon=None):
#    return util.for_positions(_earth.altitude, pos, lon)

#@rpc_handler.register('earth.radius')
#def earth_radius(pos, lon=None):
#    return util.for_positions(_earth.radius, pos, lon)

@rpc_handler.register('moon.altitude')
def moon_altitude(pos, lon=None):
    return util.for_positions(_moon.altitude, pos, lon)

@rpc_handler.register('moon.radius')
def moon_radius(pos, lon=None):
    return util.for_positions(_moon.radius, pos, lon)

@rpc_handler.register('mars.altitude')
def mars_altitude(pos, lon=None):
    return util.for_positions(_mars.altitude, pos, lon)

@rpc_handler.register('mars.radius')
def mars_radius(pos, lon=None):
    return util.for_positions(_mars.radius, pos, lon)
