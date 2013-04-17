#!/usr/local/bin/python2.7
import sys
import pika
import gzip
import base64
import random
import urllib2
import simplejson
from datetime import datetime

IMAGE_ROOT = "/heritrix/output/images/"
SERVICE = "http://flock.bl.uk/webtools/imageurls/"
HOST = "localhost"
QUEUE = "phantomjs"

def callback( ch, method, properties, body ):
        try:
        	url, dir = body.split( "|" )
                result = urllib2.urlopen( SERVICE + url )
		decoded = simplejson.loads( result.read() )[ 0 ]
		#Handle image
		image = base64.b64decode( decoded[ "image" ] )
		filename = IMAGE_ROOT + str( datetime.now().strftime( "%s" ) ) + str( random.randint( 0, 10000 ) ) + ".jpg"
		file = open( filename, "wb" )
		file.write( image )
		file.close()
		#Handle URLs
		urls = decoded[ "urls" ]
                filename = dir + str( datetime.now().strftime( "%s" ) ) + ".schedule.gz"
                file = gzip.open( filename, "wb" )
                for line in iter( urls.splitlines() ):
                	if not line.startswith( "data:" ):
                        	file.write( "F+ " + line + " E " + url + "\n" )
                file.close()
        except Exception as e:
                sys.stderr.write( "ERROR: " + str( e ) + "\n" )

while True:
	try:
		connection = pika.BlockingConnection( pika.ConnectionParameters( HOST ) )
		channel = connection.channel()
		channel.queue_declare( queue=QUEUE )
		channel.basic_consume( callback, queue=QUEUE, no_ack=True )
		channel.start_consuming()
	except Exception as e:
		sys.stderr.write( "ERROR: " + str( e ) + "\n" )
