from django.conf.urls.defaults import *

from django.contrib import admin
admin.autodiscover()

CACHE_PERIOD = 60 * 60 * 24

urlpatterns = patterns( 'phantomjs.views',
	( r'^image/(?P<url>.*)$', 'get_image' ),
	( r'^urls/(?P<url>.*)$', 'get_urls' ),
	( r'^traffic/(?P<url>.*)$', 'get_raw' ),
	( r'^imageurls/(?P<url>.*)$', 'get_image_and_urls' ),
)
