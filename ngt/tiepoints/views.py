from django.http import HttpResponse
from django.shortcuts import render_to_response
from ngt.tiepoints.models import *

def index(request):
    tiepoint_list = TiePoint.objects.all()
    if ( len(tiepoint_list) == 0 ):
        return HttpResponse("There are currently 0 tiepoints!")
    else:
        return render_to_response('tiepoints/tiepoints.html',
                                  {'tiepoint_list': tiepoint_list})
