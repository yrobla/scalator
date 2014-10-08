#This script asks a Revelator to activate or deactivate languages, and start or stop listening to queues.

import sys
import pysftp
import multiprocessing
import skiff

def dropletname_to_ip(name):

    #Authenticate with Digital Ocean
    s = skiff.rig('3461da98023e93f0a40e1058092314fbbd5cf057ceb1f1ae1e4d2d6d0d6e2209');    

    for droplet in s.Droplet.all():    

        if droplet.name == name:
            return droplet.v4[1].ip_address;

def stop_background_servers(c,lang):

	if lang == 'eng':
		processes = ['/usr/local/bin/timblserver --config=/root/fowlt/servers/timblservers/confusibles.conf --pidfile=/root/fowlt/servers/pid'];

	elif lang == 'nld':
		processes = ['/usr/local/bin/timblserver --config=/root/valkuil/valkuil-servers/confusibles.conf'];

	for process in processes:
		r = c.execute('pkill -f \''+process+'\'');


def add_language(c,lang):

	#Add language to the config file
        c.execute('python /root/tools/edit_languages.py add '+lang);

def remove_language(c,lang):
	
	if lang == 'nld':
		corrector = 'valkuil';
	elif lang == 'eng':
		corrector = 'fowlt';

	if lang in ['nld','eng']:
	        #Stop the script
	        c.execute('pkill -f \'python /root/rabbit_receiver.py '+corrector+'\'');

		#Stop related background servers
		stop_background_servers(c,lang);

	#Remove language from the config file
        c.execute('python /root/tools/edit_languages.py remove '+lang);

try:
	name = sys.argv[1];
except IndexError:
	print('python transform.py <revelator_name> +l:<language_code>');
	exit();

host = dropletname_to_ip(name);
c = pysftp.Connection(host,username='root');
print(c);

for arg in sys.argv:
	identifier = arg[:3];
	if identifier == '+l:':
		add_language(c,arg[3:]);
	elif identifier == '-l:':
		remove_language(c,arg[3:]);
	elif identifier == '+q:':
		pass;
	elif identifier == '-q:':
		pass;
