from django.http import HttpResponse
from django.shortcuts import render_to_response

from ngt.assets.models import ImageAsset

def list(request):
    assets = ImageAsset.objects.all()
    return render_to_response('assets/list_assets.html', {'assets': assets})

def show(request, asset_id):
    asset = ImageAsset.objects.get(pk=asset_id)
    return render_to_response('assets/show_asset.html', {'asset': asset})
