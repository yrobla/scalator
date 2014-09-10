#This script executes the fire_revelator.py script on the server

import pysftp
import sys
import skiff

def dropletname_to_ip(name):

    #Authenticate with Digital Ocean
    s = skiff.rig('3461da98023e93f0a40e1058092314fbbd5cf057ceb1f1ae1e4d2d6d0d6e2209');    

    for droplet in s.Droplet.all():    

        if droplet.name == name:
            return droplet.v4[1].ip_address;

try:
	name = sys.argv[1];
except IndexError:
	print('python fire_revelator.py <revelator_name>');
	exit();

host = dropletname_to_ip(name);
c = pysftp.Connection(host,username='root');
r = c.execute('python /root/startstop/fire_revelator.py');
print(r);
