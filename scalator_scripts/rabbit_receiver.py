#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pika
import time
import datetime
import subprocess
import sys
import traceback
import logging_queue
import pynlpl.formats.folia as folia
import os
import os.path
import startstop.fire_revelator

#Some basic configurations
c = open('/root/rabbit_config').readlines();
prefixes = c[1].split()[-1].split(',');
host = c[2].split()[-1];
username = c[3].split()[-1];
password = c[4].split()[-1];

credentials = pika.PlainCredentials(username, password)
parameters = pika.ConnectionParameters(credentials=credentials,host=host)

#English or Dutch?
if sys.argv[1] == 'fowlt':
    lang='en';
elif sys.argv[1] == 'valkuil':
    lang='nl';
elif sys.argv[1] == 'ucto-es':
    lang='es';

#Create the basic connection and declare the queue you're gonna use
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

for prefix in prefixes:
    if prefix == '_':
        prefix = '';
    for q in ['in','out']:
        channel.queue_declare(queue=prefix+'Revelator_'+q+'_queue_'+lang,durable=True)

#Do this when you get a message
def callback(ch, method, props, body):
    print " [x] Received %r" % (body,)    

    #A cheat to see how crashing is handled
    if body == '!!crash!!':
        asdf;

    #Add the BUSY fag
    file('/root/startstop/BUSY','w');

    #Save the document
    try:
        docname = 'd'+props.correlation_id.replace(':','');
        open('/root/revisator/input/'+docname,'w').write(body.encode('utf-8'));
    except UnicodeDecodeError:
        print('Skipping message');
        ch.basic_ack(delivery_tag = method.delivery_tag)        
        return; #We only support Unicode

    #If Spanish, only call Ucto
    if lang == 'es':
        cmd = '/usr/local/bin/ucto -X --id '+docname+' -L es /root/revisator/input/'+docname;
        result = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE).stdout.read();

        #Adjust the xml, so it supports language metadata
        foliadoc = folia.Document(string=result);
        foliadoc.language(value='es');
        result = foliadoc.xmlstring();

        print('result',result);

    #Call a spelling corrector
    else:
        subprocess.call('correct '+sys.argv[1]+' /root/revisator/input/'+docname+' /root/revisator/output/'+docname,shell=1);
        result = open('/root/revisator/output/'+docname).read();

    #Figure out the name of the queue to post the results to
    out_queue = method.routing_key.replace('_in_','_out_');

    open('out','w').write(result);

    #Give back the results
    ch.basic_publish(exchange='',
                     routing_key=out_queue,
                     properties=pika.BasicProperties(
                        correlation_id = props.correlation_id,
                        delivery_mode = 2, # make message persistent
                     ),
                     body=result)

    ch.basic_ack(delivery_tag = method.delivery_tag)
    print('Sent back result to queue '+out_queue);

    #Remove the BUSY flag
    os.remove('/root/startstop/BUSY');

    #Check if this Revelator should kill itself
    if os.path.isfile('/root/startstop/STOP'):
       startstop.fire_revelator.suicide();
       quit();

#Start listening to all queues desired, and log your crashes
try:
    channel.basic_qos(prefetch_count=1)
    for prefix in prefixes:
	if prefix == '_':
            prefix = '';
        channel.basic_consume(callback,queue=prefix+'Revelator_in_queue_'+lang,no_ack=False)
    channel.start_consuming()
except:
    t = traceback.format_exc();
    logging_queue.send(t);
    print(t);
