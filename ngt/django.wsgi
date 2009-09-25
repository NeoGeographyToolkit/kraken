import os
import sys

import django.core.handlers.wsgi

# set workspace one directory above project directory
# in most cases this will be '/data/django'
workspace = os.path.dirname(os.path.dirname(__file__))
sys.path.append(workspace)

os.environ['DJANGO_SETTINGS_MODULE'] = 'ngt.settings'

application = django.core.handlers.wsgi.WSGIHandler()
