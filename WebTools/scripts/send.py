#!/usr/local/bin/python2.7
import sys
import pika

connection = pika.BlockingConnection( pika.ConnectionParameters( "localhost" ) )
channel = connection.channel()
channel.queue_declare( queue="phantomjs" )
channel.basic_publish( exchange="", routing_key="phantomjs", body=sys.argv[ 1 ] )
connection.close()

