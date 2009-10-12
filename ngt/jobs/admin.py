from django.contrib import admin
from pds.models import Asset
from ngt.jobs.models import JobSet, Job

for model in (JobSet, Job):
    admin.site.register(model)