"""
Test Job Dependencies
"""

from ngt.jobs.models import *
from ngt.assets.models import *

def build(n=5):
    '''
    assets = []
    for i in range(n):
        a = Asset(product_id='prod%d'%i, class_label='spurious', relative_file_path='tmp/')
        a.save()
        assets.append(a)
    '''
    js = JobSet(name='Dependency Test', command='test', active=True)
    js.save()
    fjords = []
    for i in range(n):
        job = Job(command='test_fjord', jobset=js)
        job.save()
        fjords.append(job)
    bjorn = Job(command='test_bjorn', jobset=js)
    bjorn.save()
    for fjord in fjords:
       bjorn.dependencies.add(fjord)
    print "Bjorn depends on %s fjords." % len(fjords)
        
    
def main():
    JobSet.objects.all().update(active=False)
    build(5)