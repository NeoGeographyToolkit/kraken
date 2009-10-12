from django.template import RequestContext
from django.shortcuts import render_to_response

def index(request):
    return render_to_response('bigboard/index.html', {}, context_instance=RequestContext(request))

def list_nodes(request):
    pass

def list_reapers(request):
    pass