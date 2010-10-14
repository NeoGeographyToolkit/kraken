
#locking facility
import thread

# handling time entries
from datetime import datetime, tzinfo, timedelta

gtLock = thread.allocate_lock() 

class UTC(tzinfo):
    def utcoffset(self, dt): return timedelta(0)
    def tzname(self,dt): return "UTC"
    def dst(self, dt): return timedelta(0)

# singelton class, because module is only imported once!
UTCtz = UTC()

def datetime_parser(value):
    if isinstance(value, basestring):
        time_str = value.strip()
    elif isinstance(value, datetime):
        return False, value
    else:  # what else?
        return False, value

    # first split off microseconds if necessary    
    time_fractional = None
    try:
        (time_reg, time_fractional) = time_str.rsplit('.')
    except ValueError:
        time_reg = time_str
    try:
        x = datetime.strptime(time_reg, "%Y-%jT%H:%M:%S")
    except ValueError:
        try:
            x = datetime.strptime(time_reg, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return (False, time_str)

    # x is a naive datetime object, tzinfo unaware
    tz = UTCtz
    x.replace(tzinfo=tz)

    # handle micro seconds if necessary
    if time_fractional is not None:
        fracpower = 6 - len(time_fractional)
        fractional = float(time_fractional) * (10 ** fracpower)
        x.replace(microsecond=int(fractional))

    # x has tzinfo, but that will be lost in the pickle serialization
    return (True, x)
