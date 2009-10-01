from django.contrib import admin
from pds.models import Asset
from ngt.jobs.models import JobBatch, Job

for model in (JobBatch, Job):
    admin.site.register(model)