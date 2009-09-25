from django.conf.urls.defaults import *
from django.contrib import admin

import pds, pds.feeds.urls
admin.autodiscover()

urlpatterns = patterns('',
    (r'^admin/', include(admin.site.urls)),
    (r'^label/', include(pds.feeds.urls), {'meta_column': 'label'}),
    (r'^index/', include(pds.feeds.urls), {'meta_column': 'index'}),
                      )
