#!/usr/local/bin/python2.7

import sys
import time
import pika
import gzip
import daemon
import urllib2
from daemonize import Daemon
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

class ReceiveDaemon( Daemon ):
	def run( self ):
		connection = pika.BlockingConnection( pika.ConnectionParameters( HOST ) )
		channel = connection.channel()
		channel.queue_declare( queue=QUEUE )
		channel.basic_consume( callback, queue=QUEUE, no_ack=True )
		channel.start_consuming()
 
if __name__ == "__main__":
	daemon = ReceiveDaemon( "/tmp/daemon-example.pid" )
	if len( sys.argv ) == 2:
		if "start" == sys.argv[ 1 ]:
			daemon.start()
		elif "stop" == sys.argv[ 1 ]:
			daemon.stop()
		elif "restart" == sys.argv[ 1 ]:
			daemon.restart()
		else:
			print "Unknown command"
			sys.exit( 2 )
		sys.exit( 0 )
	else:
		print "usage: %s start|stop|restart" % sys.argv[ 0 ]
		sys.exit( 2 )
