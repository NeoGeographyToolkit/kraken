from django.db import models
import datetime, os, shutil

ASSET_PATH = 'static/assets'
CACHE_PATH = 'static/cache'

# Model for a generic asset
class ImageAsset(models.Model):
    date_added = models.DateTimeField('date acquired')
    asset_filename = models.CharField(max_length=200)
    cache_filename = models.CharField(max_length=200)

    def __unicode__(self):
        return self.asset_filename

    def asset_path(self):
        path =  ASSET_PATH + '/' +  (str(self.date_added.year) + '_' +
                                     str(self.date_added.month) + '_' + 
                                     str(self.date_added.day) + '/')
        if not os.path.exists(path):
            os.mkdir(path)
        return path


    def cache_path(self):
        path = CACHE_PATH + '/' +  (str(self.date_added.year) + '_' +
                                    str(self.date_added.month) + '_' + 
                                    str(self.date_added.day) + '/')
        if not os.path.exists(path):
            os.mkdir(path)
        return path
    
    @property
    def cache_file_path(self):
        return self.cache_path() + self.cache_filename

    def acquire_asset(self, filename):

        # Record the time when the asset was acquired
        print 'Acquiring ' + filename
        self.date_added = datetime.datetime.now()

        # Copy the file into the asset archive
        print '  --> Asset path: ' + self.asset_path()
        shutil.copyfile(filename, self.asset_path() + os.path.basename(filename))

        # Store the asset filename in the database
        self.asset_filename = os.path.basename(filename)
        print '  --> Asset name: ' + self.asset_filename

    def build_cache(self):
        cmd = "assets/seadragon.py " + (self.asset_path() +
                                 self.asset_filename +
                                 " -p " +
                                 self.cache_path())
        print "Building cache for " + self.asset_filename
        print "  --> " + cmd

        # Only run the command if the cache filename is empty, meaning
        # we have never built the cache.
        if self.cache_filename == "":
            os.system(cmd)

        # Formulate and store the cache filename in the database
        idx = self.asset_filename.rfind('.')
        self.cache_filename = self.asset_filename[0:idx] + ".dzi"
        print "  --> Cache name: " + self.cache_filename
