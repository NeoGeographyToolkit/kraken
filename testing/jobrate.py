#!/usr/bin/env python2.6
import sys, logging
from datetime import datetime, timedelta
import optparse
sys.path.insert(0, '..')
from ngt.dispatch.services import DispatchService
logging.getLogger('amqprpc').setLevel(logging.WARNING)

print "init"

dispatch = DispatchService(reply_queue='reply.throughput_test')
reaper_id = '00000000'
report_interval = timedelta(seconds=2)

def testloop():
    jobcount = 0
    t0 = datetime.now()
    while True:
        tr0 = datetime.now()
        dispatch.get_a_job(reaper_id)
        jobcount += 1
        dtr = datetime.now() - tr0
        if options.stopwatch:
            print "Got a job in %s" % str(dtr)
        dt = datetime.now() - t0
        if dt > report_interval:
            if options.throughput:
                print "Throughput: %f jobs/sec" % (jobcount / (dt.seconds + dt.microseconds * 1e-6))
            jobcount = 0
            t0 = datetime.now()
        
        
       
if __name__ == '__main__':
    global options
    parser = optparse.OptionParser()
    parser.add_option('-s','--stopwatch', action='store_true', dest='stopwatch', help="Time individual requests")
    parser.set_defaults(stopwatch=False, throughput=True)
    (options, args) = parser.parse_args()
    testloop()
