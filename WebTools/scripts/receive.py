#!/usr/bin/python

import pika
import urllib2
from datetime import datetime

SERVICE = "http://flock.bl.uk/traffic/urls/"
HOST = "localhost"
QUEUE = "phantomjs"

def callback( ch, method, properties, body ):
	url, dir = body.split( "|" )
	print "Received: " + url
	try:
		result = urllib2.urlopen( SERVICE + url )
		filename = dir + str( datetime.now().strftime("%s") ) + ".schedule"
		file = open( filename, "w" )
		for line in iter( result.read().splitlines() ):
			file.write( "F+ " + line + " E " + url + "\n" )
		file.close()
	except Exception as e:
		print "ERROR: " + e

connection = pika.BlockingConnection( pika.ConnectionParameters( HOST ) )
channel = connection.channel()
channel.queue_declare( queue=QUEUE )
channel.basic_consume( callback, queue=QUEUE, no_ack=True )
channel.start_consuming()
