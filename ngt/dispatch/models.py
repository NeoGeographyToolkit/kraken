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

        
class Reaper(models.Model):
    class Meta:
        app_label = 'dispatch'
        
    uuid = models.CharField(max_length=32, unique=True)
    type = models.CharField(max_length=128, default='reaper')
    start_time = models.DateTimeField(auto_now_add=True)
    #node = models.ForeignKey(Node)
    ip = models.IPAddressField(null=True)
    jobcount = models.IntegerField(default=0)