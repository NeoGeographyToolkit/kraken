from django.conf.urls.defaults import *
from pds.feeds import views

urlpatterns = patterns('',
    (r'^instrument_id/(?P<instrument_id>[\w\.-]+)/product_id/(?P<product_id>[\w\.-]+)\.(?P<format>(json|yaml))$', 
views.metadata),
    (r'^dataset_id/(?P<dataset_id>[\w\.-]+)/product_id/(?P<product_id>[\w\.-]+)\.(?P<format>(json|yaml))$', views.metadata),
                      )
