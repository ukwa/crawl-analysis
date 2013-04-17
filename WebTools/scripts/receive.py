#!/usr/bin/python

import sys
import pika
import gzip
import urllib2
from datetime import datetime

SERVICE = "http://flock.bl.uk/webtools/urls/"
HOST = "localhost"
QUEUE = "phantomjs"

def callback( ch, method, properties, body ):
	try:
		url, dir = body.split( "|" )
		result = urllib2.urlopen( SERVICE + url )
		filename = dir + str( datetime.now().strftime( "%s" ) ) + ".schedule.gz"
		file = gzip.open( filename, "wb" )
		for line in iter( result.read().splitlines() ):
			if not line.startswith( "data:" ):
				file.write( "F+ " + line + " E " + url + "\n" )
		file.close()
	except Exception as e:
		sys.stderr.write( "ERROR: " + str( e ) + "\n" )

connection = pika.BlockingConnection( pika.ConnectionParameters( HOST ) )
channel = connection.channel()
channel.queue_declare( queue=QUEUE )
channel.basic_consume( callback, queue=QUEUE, no_ack=True )
channel.start_consuming()
