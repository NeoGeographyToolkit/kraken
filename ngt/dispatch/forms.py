from django import forms
from ngt.messaging.amq_config import commands
from ngt.jobs.models import JobSet


class JobForm(forms.Form):
    commandnames = [(k,k) for k in commands.keys()]
    command = forms.ChoiceField(commandnames)
    params = forms.Field()
    
class JobSetForm(forms.ModelForm):
    class Meta:
        model = JobSet