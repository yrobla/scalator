#This script asks a Revelator to activate or deactivate languages, and start or stop listening to queues.

import sys
import pysftp
import multiprocessing
import skiff

def dropletname_to_ip(name):

    #Authenticate with Digital Ocean
    skiff.token('3461da98023e93f0a40e1058092314fbbd5cf057ceb1f1ae1e4d2d6d0d6e2209');    

    for droplet in skiff.Droplet.all():    

        if droplet.name == name:
            return droplet.v4[1].ip_address;

def timeout(func, args=(), kwargs={}, timeout_duration=3, default=None):
    import signal

    class TimeoutError(Exception):
        pass

    def handler(signum, frame):
        raise TimeoutError()

    # set the timeout handler
    signal.signal(signal.SIGALRM, handler) 
    signal.alarm(timeout_duration)
    try:
		print(1);
		result = func(*args, **kwargs)
		print(2);
    except TimeoutError as exc:
		print(3);
		result = default
		print(4);
    finally:
		print(5);
		signal.alarm(0)
		print(6);

    return result

def start_background_servers(c,lang):
	
	if lang == 'eng':
		for command in ['timblserver --config=/root/fowlt/servers/timblservers/confusibles.conf --pidfile=/root/fowlt/servers/pid &',]:
                                #'python /root/fowlt/servers/start_woprserver.py &']:
			print('Started',command);
			c.execute(command);
			print('Finished',command);

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

	#Start related background servers
#	start_background_servers(c,lang);

def remove_language(c,lang):
	
	if lang == 'nld':
		corrector = 'valkuil';
	elif lang == 'eng':
		corrector = 'fowlt';

        #Stop the script
        c.execute('pkill -f \'python /root/rabbit_receiver.py '+corrector+'\'');

	#Remove language from the config file
        c.execute('python /root/tools/edit_languages.py remove '+lang);

	#Stop related background servers
	stop_background_servers(c,lang);

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
