from django.http import HttpResponse
from django.shortcuts import render_to_response
from ngt.messaging.queue import MessageBus
from ngt.dispatch.forms import JobForm

mbus = MessageBus()

def index(request):
    return HttpResponse('Hi from the Master Control Program.')

def initiate_job(command, args = []):
    msg = ' '.join([command] + args)
    return mbus.basic_publish(msg, routing_key='command')
    
def test_view(request):
    s = initiate_job('ls -l')
    return HttpResponse(str(s))

def jobber(request):
    form = JobForm()
    if request.META['REQUEST_METHOD'] == 'GET':
        return render_to_response('dispatch/jobform.html', {'form':form})
    elif request.META['REQUEST_METHOD'] == 'POST':
        #start the job
        initiate_job(request.POST['command'], request.POST['params'].split(' '))
        return render_to_response('dispatch/jobform.html', {'form':form, 'message':'Job enqueued.'})