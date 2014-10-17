import psutil
import subprocess
import datetime
import logging_queue
import startstop
import os.path
import startstop.fire_revelator

languages = open('/root/rabbit_config').readlines()[0].split()[1].split(',');

valkuil_running = False;
fowlt_running = False;
ucto_es_running = False;

#Go through all processes currently running
for p in psutil.get_pid_list():
    
    #Get the last argument of the command that started this process
    try:
        last_argument = psutil.Process(p).cmdline[-1];
    except IndexError:
        continue;

    #If it 'Valkuil' or 'Fowlt', we know these two are running
    if last_argument == 'valkuil':
        valkuil_running = True;
    elif last_argument == 'fowlt':
        fowlt_running = True;
    elif last_argument == 'ucto-es':
        ucto_es_running = True;

#Reactivate them if they're not running
f = open('cronlog','a');
time = str(datetime.datetime.now());

if 'nld' in languages:
    if not valkuil_running:
        f.write(time+': Valkuil not running; restarting\n');
        subprocess.call('service revelator_dutch_rabbit start',shell=1);
        logging_queue.send(time+': Valkuil restarted');

	#Also start Timbl and WOPR
        r = subprocess.call('/usr/local/bin/timblserver --config=/root/valkuil/valkuil-servers/confusibles.conf &',shell=1);
	r = subprocess.call('/usr/local/bin/wopr -r server_sc -p ibasefile:/root/valkuil/valkuil-servers/ValkuilCorpus.1.1.tok.1Mlines.txt.l3r3_-a1+D.ibase,timbl:"-a1 +D",lexicon:/root/valkuil/valkuil-servers/ValkuilCorpus.1.1.tok.1Mlines.txt.lex,port:2001,keep:1,mwl:5,max_distr:250 &',shell=1);
    else:
        f.write(time+': Valkuil seems to be running fine\n');

if 'eng' in languages:
    if not fowlt_running:
        f.write(time+': Fowlt not running; restarting\n');
        subprocess.call('service revelator_english_rabbit start',shell=1);
        logging_queue.send(time+': Fowlt restarted');

	#Also start Timbl and WOPR
        r = subprocess.call('/usr/local/bin/timblserver --config=/root/fowlt/servers/timblservers/confusibles.conf --pidfile=/root/fowlt/servers/pid &',shell=1);
        r = subprocess.call('/usr/local/bin/wopr -r server_sc -p ibasefile:/root/fowlt/servers/woprserver/BNC_small.ibase,timbl:"-a1 +D",lexicon:/root/fowlt/servers/woprserver/BNC_small.lex,port:3001,keep:1,max_distr:250 &',shell=1);

    else:
        f.write(time+': Fowlt seems to be running fine\n');

if 'spa' in languages:
    if not ucto_es_running:
        f.write(time+': Ucto-es not running; restarting\n');
        subprocess.call('python /root/rabbit_receiver.py ucto-es &',shell=1);
        logging_queue.send(time+': Ucto-es restarted');

    else:
        f.write(time+': Ucto-es seems to be running fine\n');

#Check if this revelator can be killed
if os.path.isfile('/root/startstop/STOP') and not os.path.isfile('/root/startstop/BUSY'):
	startstop.fire_revelator.suicide();
	quit();

