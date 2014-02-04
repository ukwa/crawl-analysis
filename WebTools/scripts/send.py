#!/usr/bin/env python

import sys
import pika
import logging

logging.basicConfig()

connection = pika.BlockingConnection( pika.ConnectionParameters( "localhost" ) )
channel = connection.channel()
channel.queue_declare( queue="phantomjs", durable=True )
channel.basic_publish( exchange="", routing_key="phantomjs", body=sys.argv[ 1 ] )
connection.close()

