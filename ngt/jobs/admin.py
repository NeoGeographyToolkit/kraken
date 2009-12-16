from django.contrib import admin
from ngt.jobs.models import JobSet, Job

for model in (JobSet, Job):
    admin.site.register(model)
