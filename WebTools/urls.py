from django.conf.urls.defaults import *
from django.views.decorators.cache import cache_page

from django.contrib import admin
admin.autodiscover()

CACHE_PERIOD = 60 * 60 * 24

urlpatterns = patterns( '',
	( r'^webtools/', include( 'phantomjs.urls' ) ),
	( r'^admin/', include( admin.site.urls ) ),
)
