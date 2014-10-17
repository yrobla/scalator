import pika
import uuid

#Create the basic connection and declare the queue you're gonna use
credentials = pika.PlainCredentials('hortensia', 'Jdid7d8fgn8V')
parameters = pika.ConnectionParameters(credentials=credentials,host='95.85.11.29')

connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='logging',durable=True)

def send(message):

    #Send some messages, include the callback queue
    corr_id = str(uuid.uuid4())

    channel.basic_publish(exchange='', routing_key='logging',body=message,
    properties=pika.BasicProperties(reply_to = 'loggin',correlation_id = corr_id))

