import os
import sys

path = '/usr/local/webtools'
if path not in sys.path:
	sys.path.append(path)

os.environ['DJANGO_SETTINGS_MODULE'] = 'settings_production'
os.environ['LANG'] = 'en_GB.utf8'

import django.core.handlers.wsgi
application = django.core.handlers.wsgi.WSGIHandler()
