from django.conf.urls.defaults import *
from django.conf import settings
from django.contrib import admin

admin.autodiscover()

urlpatterns = patterns('',
                       (r'^(\+\+.*\+\+/)?geo', 'ngt.geo.views.rpc_handler'),
                       (r'^tiepoints/$', 'ngt.tiepoints.views.index'),
                       (r'^assets/?$', 'ngt.assets.views.list'),
                       (r'^assets/(\d+)$', 'ngt.assets.views.'),
                       (r'^static/(?P<path>.*)$', 'django.views.static.serve', {'document_root': settings.MEDIA_ROOT }),
                       (r'^admin/(.*)', admin.site.root),
                       
                       #Job Management
                       (r'^mastercontrol/?$', 'mastercontrol.views.index'),
                       (r'^mastercontrol/job/?$', 'mastercontrol.views.jobber'),
                       
                       #(r'^(.*)$', 'ngt.views.index'),
                       (r'^/?$', 'ngt.views.index'),
                      
)
