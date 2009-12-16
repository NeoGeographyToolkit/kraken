from ngt.jobs.models import *
from ngt.assets.models import *
def test():
    jb = JobSet.objects.latest('pk')
    jb.execute()

def build(n):
    assets = []
    for i in range(n):
        a = Asset(product_id='prod%d'%i, class_label='spurious', relative_file_path='tmp/')
        a.save()
        assets.append(a)
    js = JobSet(name='Fake JobSet', command='test', active=True)
    js.save()
    for a in assets:
        js.assets.add(a)
    js.simple_populate(creates_new_asset=False)
