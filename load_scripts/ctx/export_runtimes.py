"""
This module contains some utilities to help export job runtimes for analysis.
There is CTX specific code here for convenience, in that it loads a list of image dimensions from a specific place, but it doesn't need to be that way.
"""
import os
import csv
import re

def to_seconds(td):
    """ td is a TimeDelta """
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6

_dimension_dict = None
def load_dimdict(tabfile="/big/sourcedata/mars/ctx/meta/image_dims.tab"):
    """
    Expects an input csv of dimensions in the form:
    product_id, samples, lines, center_latitude, center_longitude
    No header.
    """
    global _dimension_dict
    if _dimension_dict:
        return _dimension_dict
    if not os.path.exists(tabfile):
        raise Exception("Dimensions table does not exist at %s" % tabfile)
    _dimension_dict = {}
    tab_reader = csv.reader(open(tabfile))
    print "Reading image dimensions table from %s" % tabfile
    for product_id, samples, lines, center_longitude, center_latitude in tab_reader:
        _dimension_dict[product_id] = (int(samples), int(lines), float(center_longitude), float(center_latitude))
    print "Done."
    return _dimension_dict

def runtime_range_filter(jobs, min_secs, max_secs):
    """ Yield only complete jobs from the given sequence that have a runtime between min_secs and max_secs (inclusive)"""
    for j in jobs:
        secs = to_seconds(j.runtime)
        if not min_secs or min_secs <= secs:
            if not max_secs or max_secs >= secs:
                yield j

def dims_and_runtimes(jobset, min_secs=None, max_secs=None):
    """ 
    CTX-Specific.  
    Generate runtimes and dimensions for completed jobs in the given JobSet.
    Yields tuples in the form:
    (product_id, runtime, samples, lines, lon, lat)
    """
    prod_id_pattern = re.compile('[A-Z0-9]{3}_\d{6}_\d{4}_X[IN]_\d{2}[NS]\d{3}W')
    dimensions = load_dimdict()

    jobs = jobset.jobs.status_filter("complete").only("id","time_started","time_ended", "context")
    print jobs.count(), " jobs."

    if min_secs or max_secs:
        jobs = runtime_range_filter(jobs, min_secs, max_secs)

    for job in jobs:
        assert job.time_ended
        url = job.arguments[0]
        product_id = prod_id_pattern.search(url).group(0)
        samples, lines, lon, lat = dimensions[product_id]

        yield (product_id, to_seconds(job.runtime), samples, lines, lon, lat)

def write_runtimes(jobset, outfilename):
    outfile = open(outfilename, 'w')
    outfile.write("productid,runtime,lines,samples,lon,lat\n")
    for tup in dims_and_runtimes(jobset):
        outfile.write(','.join(str(v) for v in tup))
        outfile.write("\n")
    outfile.close()
