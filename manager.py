import logging
import paramiko
import threading
import time
import nodeutils as utils
from task_manager import Task, TaskManager, ManagerStoppedException
import skiff
from skiff.Image import SkiffImage

SERVER_LIST_AGE = 5   # How long to keep a cached copy of the server list
ITERATE_INTERVAL = 2  # How long to sleep while waiting for something
                      # in a loop

def iterate_timeout(max_seconds, purpose):
    start = time.time()
    count = 0
    while (time.time() < start + max_seconds):
        count += 1
        yield count
        time.sleep(ITERATE_INTERVAL)
    raise Exception("Timeout waiting for %s" % purpose)

class NotFound(Exception):
    pass

class CreateServerTask(Task):
    def main(self, client):
        # Try to find the image ID for the version specified
        images= client.Image.all();
        for image in images:
            if image.name == 'Revelator v'+str(self.args["version"]):
                image_id = image.id
                break

        # Create the droplet
        droplet_name = 'rev.'+self.args['name']
        droplet = client.Droplet.create(name=droplet_name, region='ams2',
            size=self.args["size"],
            image=image_id, private_networking=True,
            ssh_keys=client.Key.all())
        droplet.wait_till_done()
        droplet = droplet.refresh()
        return make_server_dict(droplet)

def make_server_dict(server):
    d = dict(id=str(server.id),
             name=server.name,
             status=server.status,
             addresses=server.v4)

    # public ip is the last one
    d['public_v4'] = server.v4[-1]
    return d

class GetServerTask(Task):
    def main(self, client):
        try:
            server = client.Droplet.get(self.args['server_id'])
        except:
            raise NotFound()
        return make_server_dict(server)

class ListServersTask(Task):
    def main(self, client):
        print "in list servers task"
        servers = client.Droplet.all()
        return [make_server_dict(server) for server in servers]

class ScalatorManager(TaskManager):
    log = logging.getLogger("scalator.ScalatorManager")

    def __init__(self, scalator):
        super(ScalatorManager, self).__init__(None)
        self.scalator = scalator
        self._client = self._getClient()
        self._servers = []
        self._servers_time = 0
        self._servers_lock = threading.Lock()

    def _getClient(self):
        # get digitalocean connection
        return skiff.rig(self.scalator.config.provider_token)

    def createServer(self, name):
        # digital ocean create server
        create_args = dict(name=name, version=self.scalator.config.provider_version, size=self.scalator.config.provider_size, client=self._client)
        return self.submitTask(CreateServerTask(**create_args))

    def getServer(self, server_id):
        print "in get server"
        print server_id
        try:
            server = self._client.Droplet.get(server_id)
        except Exception as e:
            print str(e)
            raise NotFound()
        return make_server_dict(server)

    def getServerFromList(self, server_id):
        for s in self.listServers():
            print s['id']
            if s['id'] == server_id:
                return s
        raise NotFound()

    def _waitForResource(self, resource_type, resource_id, timeout):
        last_status = None
        for count in iterate_timeout(timeout,
                                     "%s %s" % (resource_type,
                                                      resource_id)):
            try:
                if resource_type == 'server':
                    resource = self.getServerFromList(resource_id)
                elif resource_type == 'image':
                    resource = self.getImage(resource_id)
            except NotFound:
                continue
            except ManagerStoppedException:
                raise
            except Exception:
                self.log.exception('Unable to list %ss while waiting for '
                                   '%s will retry' % (resource_type,
                                                      resource_id))
                continue

            status = resource.get('status')
            if (last_status != status):
                self.log.debug('Status of %s %s: %s' %
                               (resource_type, resource_id, status))
            last_status = status
            if status in ['ACTIVE', 'ERROR']:
                return resource

    def waitForServer(self, server_id, timeout=3600):
        return self._waitForResource('server', server_id, timeout)

    def waitForServerDeletion(self, server_id, timeout=600):
        for count in iterate_timeout(600, "server %s deletion" %
                                     (server_id)):
            try:
                self.getServerFromList(server_id)
            except NotFound:
                return

    def listServers(self):
        if time.time() - self._servers_time >= SERVER_LIST_AGE:
            # Since we're using cached data anyway, we don't need to
            # have more than one thread actually submit the list
            # servers task.  Let the first one submit it while holding
            # a lock, and the non-blocking acquire method will cause
            # subsequent threads to just skip this and use the old
            # data until it succeeds.
            if self._servers_lock.acquire(False):
                try:
                    print "submit task list servers"
                    self._servers = self.submitTask(ListServersTask())
                    self._servers_time = time.time()
                finally:
                    self._servers_lock.release()
        return self._servers

    def cleanupServer(self, server_id, ip):
        done = False
        while not done:
            try:
                # login by ssh into the server and send flag to delete
                connect_kwargs = dict(key_filename=self.scalator.config.private_key)
                host = utils.ssh_connect(ip, self.scalator.config.private_user,
                    connect_kwargs=connect_kwargs, timeout=600)
                if not host:
                    raise Exception("Unable to log in via SSH")

                # write a flag file
                host.ssh("send delete flag file", "touch /root/startstop/STOP")

                done = True
            except NotFound:
                # If we have old data, that's fine, it should only
                # indicate that a server exists when it doesn't; we'll
                # recover from that.  However, if we have no data at
                # all, wait until the first server list task
                # completes.
                if self._servers_time == 0:
                    time.sleep(SERVER_LIST_AGE + 1)
                else:
                    done = True

        # This will either get the server or raise an exception
        self.log.debug('Deleting server %s' % server_id)
