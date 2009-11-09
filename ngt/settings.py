import os, sys, platform
import logging
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger()

#Import local settings file if it exists and set HOST dictionary
try:
    #import local_settings
    #HOST     = local_settings.HOST
    from local_settings import *
    HOSTNAME = platform.node()
except:
    HOST = {
        'url_prefix':'/ngt',
        'static_prefix':'/static/ngt',
        'db_engine':'postgresql_psycopg2',
        'db_name':'ngt',
        'db_user':'django',
        'db_host':'127.0.0.1',
        'db_port':'31337'}

DEVELOPMENT_MODE = True


# Django settings for ngt project.

DEBUG = DEVELOPMENT_MODE
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    ('Michael Broxton', 'michael.broxton@nasa.gov'),
)
MANAGERS = ADMINS

#APP_BASE is where this settings.py file is, all other paths will be specified relative to this
if __name__ == '__main__':
    filename = sys.argv[0]
else:
    filename = __file__
APP_BASE = os.path.abspath(os.path.dirname(filename))
REPO_BASE = os.path.dirname(APP_BASE)
sys.path.insert(0, REPO_BASE)

# For now we are using a simple little sqlite database!
#
DATABASE_ENGINE = '%s' % HOST['db_engine']      
DATABASE_NAME = '%s' % HOST['db_name']
DATABASE_USER = '%s' % HOST['db_user']
DATABASE_HOST = '%s' % HOST['db_host']
DATABASE_PORT = '%s' % HOST['db_port']
DATABASE_PASSWORD = HOST.get('db_password', None)
if not DATABASE_PASSWORD and HOSTNAME  == 'byss.arc.nasa.gov':
        _f = open("%s/db.pwd" % APP_BASE, "r")
        DATABASE_PASSWORD = _f.read()
        _f.close()

#DATABASE_ENGINE = 'sqlite3'
#DATABASE_NAME = os.path.join(APP_BASE, 'database', 'ngt.sqlite')
#DATABASE_USER = ''             # Not used with sqlite3.
#DATABASE_PASSWORD = ''         # Not used with sqlite3.
#DATABASE_HOST = ''             # Set to empty string for localhost. Not used with sqlite3.
#DATABASE_PORT = ''             # Set to empty string for default. Not used with sqlite3.

#Template tags
URL_PREFIX	= HOST['url_prefix']
STATIC_PREFIX	= HOST['static_prefix']

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# If running in a Windows environment this must be set to the same as your
# system time zone.
TIME_ZONE = "PST+08PDT,M3.2.0,M11.1.0"

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = False

# Absolute path to the directory that holds media.
# Example: "/home/media/media.lawrence.com/"
MEDIA_ROOT = os.path.join(APP_BASE, 'static')

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash if there is a path component (optional in other cases).
# Examples: "http://media.lawrence.com", "http://example.com/media/"
MEDIA_URL = '/static/'

# URL prefix for admin media -- CSS, JavaScript and images. Make sure to use a
# trailing slash.
ADMIN_MEDIA_PREFIX = STATIC_PREFIX + '/admin/'

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'x4^=emb*r4fl9gi=+lu=76@7@l&tle-+h_d!z0q7xd$ym6@8cr'

# List of callables that know how to import templates from various sources.
TEMPLATE_LOADERS = (
    'django.template.loaders.filesystem.load_template_source',
    'django.template.loaders.app_directories.load_template_source',
)

MIDDLEWARE_CLASSES = (
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
)

ROOT_URLCONF = 'ngt.urls'
APPEND_SLASH = True

AUTHENTICATION_BACKENDS = (
	# django default authentication backend
	'django.contrib.auth.backends.ModelBackend',

	# backend to allow remote_user from apache - must uncomment middleware above and restrict url in apache conf
	#'ngt.authbackends.apache.ApacheBackend',

	# backend to django app to authenticate directly against kerberos service - uncomment krb settings below
	#'ngt.authbackends.krb.Krb5Backend',
)

# Krb settings
#KEYTAB = '/http-krb5.keytab'
#SERVICE = 'HTTP'

TEMPLATE_DIRS = (
     os.path.join(APP_BASE, 'templates'),
)

# Settings for GeoDjango
# Add your GEOS and GDAL path to the lists!
def search(paths):
    for path in paths:
        if os.path.exists(path):
            return path
    return None
GEOS_LIBRARY_PATH = search(('/data/local/lib/libgeos_c.so', '/opt/local/lib/libgeos_c.dylib'))
GDAL_LIBRARY_PATH = search(('/data/local/lib/libgdal.so', '/Applications/Google Earth EC.app/Contents/MacOS/libgdal.dylib'))

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.admin',
    #'django.contrib.gis',
    'ngt.tiepoints',
    'ngt.assets',
    'ngt.jobs',
    'ngt.dispatch',
    'ngt.django_extras',
    'pds',
)

#HACK ATTACK... FIXME
#Did the below so I can develop locally without GEOS installed
#--ebs
try:
    from django.contrib.gis.admin.options import GeoModelAdmin
    print "Adding django.contrib.gis to INSTALLED_APPS."
    INSTALLED_APPS += ('django.contrib.gis',)
except ImportError:
    logger.warning("Could not import GeoModelAdmin. (GEOS may be missing).  geodjango will be disabled.")
