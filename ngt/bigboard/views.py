from django.template import RequestContext
from django import http
from django.shortcuts import render_to_response
from django.core.urlresolvers import reverse
from django import forms
from ngt.dispatch.models import Reaper
from ngt.jobs.models import JobSet
from ngt.dispatch.forms import JobSetForm

def index(request):
    return render_to_response('bigboard/index.html', {}, context_instance=RequestContext(request))

def list_nodes(request):
    pass

def list_reapers(request):
    reapers = Reaper.objects.all()
    return render_to_response('bigboard/reaperlist.html', {'reapers':reapers}, context_instance=RequestContext(request))

def list_jobsets(request):
    jobsets = JobSet.objects.all()
    return render_to_response('bigboard/jobsetlist.html', {'jobsets':jobsets}, context_instance=RequestContext(request))

def jobset_detail(request, jobset_id=None):
    if request.method == 'POST':
        #update
        js = JobSet.objects.get(pk=jobset_id)
        jsf = JobSetForm(request.POST, instance=js)
        try:
            jsf.save()
            return http.HttpResponseRedirect(reverse(list_jobsets))
        except ValueError:
            #form didn't validate
            #errors in jsf.errors
            pass

    else:
        js = JobSet.objects.get(pk=jobset_id)
        jsf = JobSetForm(instance=js)

    return render_to_response('bigboard/jobsetdetail.html', {'jobsetform':jsf}, context_instance=RequestContext(request))
    
def jobset_create(request):
    if request.method == 'POST':
        #create
        jsf = JobSetForm(request.POST)
        try:
            jsf.save()
            return http.HttpResponseRedirect(reverse(list_jobsets))
        except ValueError:
            #form didn't validate
            #errors are in jsf.errors
            pass
    else:
        #blank form
        jsf = JobSetForm()
        return render_to_response('bigboard/jobsetdetail.html', {'jobsetform':jsf}, context_instance=RequestContext(request))
        