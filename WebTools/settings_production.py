from settings import *

DEBUG = TEMPLATE_DEBUG = True

DATABASES = {
    'default': {
	'ENGINE': 'django.db.backends.sqlite3',
	'NAME': '/usr/local/siteimage/sqlite3.db',
	'USER': '',
	'PASSWORD': '',
	'HOST': '',
	'PORT': '',
    }
}

ROOT_URLCONF = 'urls'

STATIC_URL = '/webtools/static/'

TEMPLATE_DIRS = (
	'/usr/local/webtools/templates',
)

SESSION_COOKIE_SECURE = True
