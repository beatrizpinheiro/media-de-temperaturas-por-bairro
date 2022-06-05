#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue_input', durable=True)

myfile = open("Data.txt", "r") 
myline = myfile.readline()

while myline:
	message = ' '.join(sys.argv[1:]) or myline
	channel.basic_publish(
	    exchange='',
	    routing_key='task_queue_input',
	    body=message,
	    properties=pika.BasicProperties(
		delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
	    ))
	print(" [x] Sent %r" % message)
	myline = myfile.readline()

connection.close()
myfile.close() 