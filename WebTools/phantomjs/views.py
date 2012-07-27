import os
import re
import logging
import random
import simplejson
from PIL import Image
from subprocess import Popen, PIPE
from django.http import HttpResponse
from phantomjs.settings import phantomjs, rasterize, netsniff, temp

logger = logging.getLogger( "phantomjs.views" )

def get_image( request, url ):
	tmp = temp + str( random.randint( 0, 100 ) ) + ".jpg"
	image = Popen( [ phantomjs, rasterize, url, tmp ], stdout=PIPE, stderr=PIPE )
	stdout, stderr = image.communicate()
	im = Image.open( tmp )
	crop = im.crop( ( 0, 0, 1280, 960 ) )
	crop.save( tmp, "JPEG" )
	data = open( tmp, "rb" ).read()
	os.remove( tmp )
	return HttpResponse( content=data, mimetype="image/jpeg" )

def strip_debug( json ):
	lines = json.splitlines()
	for index, line in enumerate( lines ):
		if line == "{":
			final= ""
			for l in lines[ index: ]:
				final = final + l + "\n"
			return final
	return lines

def get_har( url ):
	har = Popen( [ phantomjs, netsniff, url ], stdout=PIPE, stderr=PIPE )
	stdout, stderr = har.communicate()
	return strip_debug( stdout )

def get_raw( request, url ):
	json = get_har( url )
	return HttpResponse( content = json, mimetype="application/json" )

def get_urls( request, url ):
	json = get_har( url )
	data = simplejson.loads( json )

	response = ""
	for entry in data[ "log" ][ "entries" ]:
		response = response + entry[ "request" ][ "url" ] + "\n"

	return HttpResponse( content = response, mimetype="text/plain" )

