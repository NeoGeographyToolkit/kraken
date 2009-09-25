from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
import os, sys

from ngt.assets.models import ImageAsset

class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        )
    help = 'Manipulates NGT Assets'
    args = '<command> [options]'

    def handle(self, *args, **options):
        if len(args) < 1:
            print help
            sys.exit(1)
        command = args[0]

        if (command == 'acquire'):
            if len(args) < 2:
                print 'you must specify a file to acquire'
                sys.exit(1)
            filename = args[1]
            a = ImageAsset()
            a.acquire_asset(filename)
            a.build_cache()
            a.save()

        else:
            print 'Unknown command: ' + command
