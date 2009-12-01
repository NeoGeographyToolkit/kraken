from django.db import models


# Let's see if we can do this without modelling Nodes...
#class Node(models.Model):
#    """ A registered node from which we can launch reapers. """
#    ip = models.IPAddressField()
#    start_time = models.DateTimeField(auto_now_add=True)
#    
#    def are_you_alive(self):
#        """ Send the node controller a message or signal to see if it is still alive."""
#        pass


class ReaperManager(models.Manager):
    ''' Manage soft deletion and expiration of reapers '''
    def get_query_set(self):
        return super(ReaperManager, self).get_query_set().filter(deleted=False, expired=False)
    def deleted(self):
        return super(ReaperManager, self).get_query_set().filter(deleted=True)
    def expired(self):
        return super(ReaperManager, self).get_queryset().filter(expired=True)
    def any(self):
        return super(ReaperManager, self).all()
        
class Reaper(models.Model):
    objects = ReaperManager()
    class Meta:
        app_label = 'dispatch'
        
    uuid = models.CharField(max_length=32, unique=True)
    type = models.CharField(max_length=128, default='generic')
    creation_time = models.DateTimeField(auto_now_add=True)
    modification_time = models.DateTimeField(auto_now=True)
    last_job_finished = models.DateTimeField(null=True, default=None)
    status = models.CharField(max_length=128, default='up')
    ip = models.IPAddressField(null=True)
    jobcount = models.IntegerField(default=0)
    
    # Soft Deletion and expiration
    deleted = models.BooleanField(default=False)
    expired = models.BooleanField(default=False)
    
    def soft_delete(self):
        self.deleted = True
        self.save()
