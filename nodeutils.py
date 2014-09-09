import errno
import time
import socket
import logging
from sshclient import SSHClient

import paramiko

log = logging.getLogger("scalator.utils")

def iterate_timeout(max_seconds, purpose):
    start = time.time()
    count = 0
    while (time.time() < start + max_seconds):
        count += 1
        yield count
        time.sleep(2)
    raise Exception("Timeout waiting for %s" % purpose)

def ssh_connect(ip, username, connect_kwargs={}, timeout=60):
    for count in iterate_timeout(timeout, "ssh access"):
        try:
            client = SSHClient(ip, username, **connect_kwargs)
            break
        except paramiko.AuthenticationException as e:
            # This covers the case where the cloud user is created
            # after sshd is up (Fedora for example)
            log.info('Password auth exception. Try number %i...' % count)
        except socket.error as e:
            if e[0] not in [errno.ECONNREFUSED, errno.EHOSTUNREACH]:
                log.exception('Exception while testing ssh access:')

    out = client.ssh("test ssh access", "echo access okay", output=True)
    if "access okay" in out:
        return client
    return None


