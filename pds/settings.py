# Django settings for copernicus project.

import sys
import os
import platform

PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))
PARENT_PATH = os.path.normpath(os.path.join(PROJECT_PATH,'..'))
sys.path.insert(0, PARENT_PATH)
sys.path.insert(1, PROJECT_PATH)
 
# local mac os x machine
if platform.system() == 'Darwin':
  GEOS_LIBRARY_PATH = 'Library/Frameworks/GEOS.framework/unix/lib/libgeos.dylib'
else:
 GEOS_LIBRARY_PATH = '/data/local/lib/libgeos_c.so'

DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    # ('Your Name', 'your_email@domain.com'),
)

MANAGERS = ADMINS

if platform.system() == 'Java':
  DATABASE_ENGINE = 'doj.backends.zxjdbc.postgresql' # for jython
else:
  DATABASE_ENGINE = 'postgresql_psycopg2' # 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
# uses kerberos authentication, make sure to have certificate, run "kinit" command
DATABASE_NAME = 'pds_pc'                # Or path to database file if using sqlite3.
DATABASE_HOST = 'bosshog'               # Set to empty string for localhost. Not used with sqlite3.
DATABASE_PORT = ''                      # Set to empty string for default. Not used with sqlite3.

#nebula connection
if platform.system() == 'Darwin':
  DATABASE_NAME = 'pds'
  DATABASE_HOST = '208.87.118.87'
  DATABASE_PORT = '80'
  DATABASE_USER = 'pds'
  DATABASE_PASSWORD = 'passthepds'

# local settings
#DATABASE_USER = 'upcmgr'                # Not used with sqlite3.
#DATABASE_PASSWORD = 'test1000'         # Not used with sqlite3.

# old local byss db settings
#DATABASE_NAME = 'testdb1'               # Or path to database file if using sqlite3.
#DATABASE_USER = 'django'                # Not used with sqlite3.
#DATABASE_PASSWORD = 'reinhardt'         # Not used with sqlite3.

try:
    from local_settings import *
except:
    pass

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# If running in a Windows environment this must be set to the same as your
# system time zone.
TIME_ZONE = 'America/Los_Angeles'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# Absolute path to the directory that holds media.
# Example: "/home/media/media.lawrence.com/"
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash if there is a path component (optional in other cases).
# Examples: "http://media.lawrence.com", "http://example.com/media/"
MEDIA_URL = 'http://byss.arc.nasa.gov/img-pds/media/'

# URL prefix for admin media -- CSS, JavaScript and images. Make sure to use a
# trailing slash.
# Examples: "http://foo.com/media/", "/media/".
ADMIN_MEDIA_PREFIX = '/img-pds/media/'

# Make this unique, and don't share it with anybody.
SECRET_KEY = '&ri_=%i-^j39p_h=qh_@)(63l_=xwc51_a-k@*#cvhm60p^5s)'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.load_template_source',
    'django.template.loaders.app_directories.load_template_source',
#     'django.template.loaders.eggs.load_template_source',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
)

ROOT_URLCONF = 'pds.urls'

TEMPLATE_DIRS = (
    # Put strings here, like "/home/html/django_templates" or "C:/www/django/templates".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
    PROJECT_PATH + '/templates',
)

# include these application models for manage.py syncdb command!
INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
#    'django.contrib.sites',
    'django.contrib.admin',
    'pds',
)
