import logging
import paramiko
import threading
import time
from task_manager import Task, TaskManager, ManagerStoppedException

SERVER_LIST_AGE = 5   # How long to keep a cached copy of the server list
ITERATE_INTERVAL = 2  # How long to sleep while waiting for something
                      # in a loop

class CreateServerTask(Task):
    def main(self, client):
        server = client.servers.create(**self.args)
        return str(server.id)

class GetServerTask(Task):
    def main(self, client):
        try:
            server = client.servers.get(self.args['server_id'])
        except novaclient.exceptions.NotFound:
            raise NotFound()
        return make_server_dict(server)

class DeleteServerTask(Task):
    def main(self, client):
        client.servers.delete(self.args['server_id'])

class ListServersTask(Task):
    def main(self, client):
        servers = client.servers.list()
        return [make_server_dict(server) for server in servers]

class ScalatorManager(TaskManager):
    log = logging.getLogger("scalator.ScalatorManager")

    def __init__(self):
        super(ScalatorManager, self).__init__(None)
        self._client = self._getClient()
        self._servers = []
        self._servers_time = 0
        self._servers_lock = threading.Lock()

    def _getClient(self):
        # get digitalocean connection
        return None

    def createServer(self, name):
        # digital ocean create server
        create_args = dict(name=name)
        return self.submitTask(CreateServerTask(**create_args))

    def getServer(self, server_id):
        return self.submitTask(GetServerTask(server_id=server_id))

    def getServerFromList(self, server_id):
        for s in self.listServers():
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
                    self._servers = self.submitTask(ListServersTask())
                    self._servers_time = time.time()
                finally:
                    self._servers_lock.release()
        return self._servers

    def deleteServer(self, server_id):
        return self.submitTask(DeleteServerTask(server_id=server_id))

    def cleanupServer(self, server_id):
        done = False
        while not done:
            try:
                server = self.getServerFromList(server_id)
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
        server = self.getServerFromList(server_id)
        self.log.debug('Deleting server %s' % server_id)
        self.deleteServer(server_id)
