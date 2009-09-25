import math
from SimpleXMLRPCServer import SimpleXMLRPCDispatcher
from django.http import HttpResponse, HttpResponseBadRequest

class XMLRPCHandler(SimpleXMLRPCDispatcher):
    def __init__(self):
        SimpleXMLRPCDispatcher.__init__(self, allow_none=False, encoding=None)

    def __call__(self, request, *args, **kwargs):
        if request.method != 'POST':
            return HttpResponseBadRequest("Operation not supported.")
        return HttpResponse(self._marshaled_dispatch(request.raw_post_data))

    def register(self, name):
        def decorator(func):
            self.register_function(func, name)
        return decorator

def isnumber(x):
    "Is x a number? We say it is if it has an __int__ method."
    return hasattr(x, '__int__')

def for_positions(func,pos,lon):
    if isnumber(pos) and isnumber(lon):
        return func(pos,lon)
    if pos.__class__ != list or lon != None:
        raise TypeError
    if len(pos) == 2 and isnumber(pos[0]) and isnumber(pos[1]):
        return func(pos[0],pos[1])
    if len(pos) > 1000:
        raise ValueError
    result = []
    for p in pos:
        if p.__class__ != list:
            raise TypeError
        if len(p) != 2:
            raise ValueError
        if not isnumber(p[0]) or not isnumber(p[1]):
            raise TypeError
        result.append(func(p[0],p[1]))
    return result


def interpolate_lat_lon(image, lat, lon, maxlat=90):
    if lat > maxlat or lat < -maxlat:
        # Should throw something here instead
        return 0
    lon = lon % 360
    if lon > 180: lon = lon - 360
    x = (lon+180)*(image.cols/360.0) - 0.5
    y = (90-lat)*(image.rows/180.0) - 0.5

    x0 = int(math.floor(x)) % image.cols
    y0 = int(math.floor(y))
    x1 = (x0+1) % image.cols
    y1 = y0+1
    if y0<0: y0=0
    if y1>=image.rows-1: y1=image.rows-1

    v00 = image[x0,y0]
    if v00 == -32768: v00 = 0
    v10 = image[x1,y0]
    if v10 == -32768: v10 = 0
    v01 = image[x0,y1]
    if v01 == -32768: v01 = 0
    v11 = image[x1,y1]
    if v11 == -32768: v11 = 0
    
    xa = x % 1
    ya = y % 1

    return (1-ya)*((1-xa)*v00+xa*v10) + ya*((1-xa)*v01+xa*v11)
