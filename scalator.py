import json
import logging
import random
import threading
import time
import yaml
import nodedb
import nodeutils as utils
import manager
import math
import pika

MINS = 60
HOURS = 60 * MINS

WATERMARK_SLEEP = 10         # Interval between checking if new servers needed
CONNECT_TIMEOUT = 10 * MINS  # How long to try to connect after a server
                             # is ACTIVE
NODE_CLEANUP = 8 * HOURS     # When to start deleting a node that is not
                             # READY or HOLD
DELETE_DELAY = 1 * MINS      # Delay before deleting a node that has completed
                             # its job.


class NodeCompleteThread(threading.Thread):
    log = logging.getLogger("scalator.NodeCompleteThread")

    def __init__(self, scalator, nodename):
        threading.Thread.__init__(self, name='NodeCompleteThread for %s' % nodename)
        self.nodename = nodename
        self.scalator = scalator

    def run(self):
        try:
            with self.scalator.getDB().getSession() as session:
                self.handleEvent(session)
        except Exception:
            self.log.exception("Exception handling event for %s:" %
                               self.nodename)

    def handleEvent(self, session):
        node = session.getNodeByNodename(self.nodename)
        if not node:
            self.log.debug("Unable to find node with nodename: %s" %
                           self.nodename)
            return

        if node.state == nodedb.HOLD:
            self.log.info("Node id: %s is complete but in HOLD state" %
                          node.id)
            return
        time.sleep(DELETE_DELAY)
        self.scalator.deleteNode(node.id)

	
class NodeDeleter(threading.Thread):
    log = logging.getLogger("scalator.NodeDeleter")

    def __init__(self, scalator, node_id):
        threading.Thread.__init__(self, name='NodeDeleter for %s' % node_id)
        self.node_id = node_id
        self.scalator = scalator

    def run(self):
        try:
            with self.scalator.getDB().getSession() as session:
                node = session.getNode(self.node_id)
                self.scalator._deleteNode(session, node)
        except Exception:
            self.log.exception("Exception deleting node %s:" %
                               self.node_id)

class NodeLauncher(threading.Thread):
    log = logging.getLogger("scalator.NodeLauncher")

    def __init__(self, scalator, node_id, timeout, launch_timeout):
        threading.Thread.__init__(self, name='NodeLauncher for %s' % node_id)
        self.node_id = node_id
        self.timeout = timeout
        self.scalator = scalator
        self.launch_timeout = launch_timeout

    def run(self):
        try:
            self._run()
        except Exception:
            self.log.exception("Exception in run method:")

    def _run(self):
        with self.scalator.getDB().getSession() as session:
            self.log.debug("Launching node id: %s" % self.node_id)
            try:
                self.node = session.getNode(self.node_id)
            except Exception:
                self.log.exception("Exception preparing to launch node id: %s:"
                                   % self.node_id)
                return

            try:
                start_time = time.time()
                dt = self.launchNode(session)
                failed = False
            except Exception as e:
                self.log.exception("%s launching node id: %s "
                                   "error:" %
                                   (e.__class__.__name__,
                                    self.node_id))
                dt = int((time.time() - start_time) * 1000)
                failed = True

            if failed:
                try:
                    self.scalator.deleteNode(self.node_id)
                except Exception:
                    self.log.exception("Exception deleting node id: %s:" %
                                       self.node_id)

    def runReadyScript(self, node):
        self.log.info("In run ready script")
        connect_kwargs = dict(key_filename=self.scalator.config.private_key)
        host = utils.ssh_connect(node.ip, self.scalator.config.private_user,
                                  connect_kwargs=connect_kwargs,
                                  timeout=self.timeout)
        if not host:
            raise Exception("Unable to log in via SSH")

        # add all languages to the node
        self.log.info("Languages are %s" % (",".join(self.scalator.config.languages)))
        for language in self.scalator.config.languages:
            host.ssh("add %s language to node" % language,
                     "python /root/tools/edit_languages.py add %s" %
                     language, output=True)


    def launchNode(self, session):
        start_time = time.time()
        timestamp = int(start_time)

        hostname = 'host-%s' % str(self.node.id)
        self.node.hostname = hostname

        self.log.info("Creating server with hostname %s for node id: %s" % (hostname, self.node_id))
        server = self.scalator.manager.createServer(hostname)
        self.node.external_id = server.get('id')
        self.node.nodename = server.get('name')
        session.commit()

        ip = server.get('public_v4')
        if not ip:
            raise LaunchNetworkException("Unable to find public IP of server")

        # now execute fire revelator
        self.node.ip = ip.ip_address
        self.log.debug("Node id: %s is running, ip: %s, testing ssh" %
                       (self.node.id, ip.ip_address))
        connect_kwargs = dict(key_filename=self.scalator.config.private_key)

        if not utils.ssh_connect(ip.ip_address, self.scalator.config.private_user,
                                 connect_kwargs=connect_kwargs,
                                 timeout=self.timeout):
            raise LaunchAuthException("Unable to connect via ssh")

        # execute ready script
        self.runReadyScript(self.node)

        # Save the elapsed time for statsd
        dt = int((time.time() - start_time) * 1000)
        self.node.state = nodedb.READY
        self.log.info("Node id: %s is ready" % self.node.id)

        return dt


class RabbitListener(threading.Thread):
    log = logging.getLogger("scalator.RabbitListener")

    def __init__(self, scalator, addr):
        threading.Thread.__init__(self, name='RabbitListner for %s' % addr)
        self.scalator = scalator
        self.addr = addr

    def run(self):
        try:
            self._run()
        except Exception:
            self.log.exception("Exception in run method:")

    def _run(self):
        # periodically listen to queue, to get total messages
        # update needed workers according to messages in queue
        total_messages = 0
        self.connection = pika.BlockingConnection(pika.URLParameters(self.addr))
        for queue in self.scalator.config.rabbit_queues:
            if self.connection:
                channel = self.connection.channel(None)
                q = channel.queue_declare(queue=queue, passive=True, durable=True, exclusive=False)
                total_messages += q.method.message_count

        self.scalator.setNeededWorkers(int(math.ceil(total_messages/self.scalator.config.messages_per_node)))
        self.connection = None
        time.sleep(WATERMARK_SLEEP)


class Scalator(threading.Thread):
    log = logging.getLogger("scalator.Scalator")

    def __init__(self, configfile, watermark_sleep=WATERMARK_SLEEP):
        threading.Thread.__init__(self, name='Scalator')
        self.configfile = configfile
        self.watermark_sleep = watermark_sleep
        self._stopped = False
        self.config = None
        self._delete_threads = {}
        self._delete_threads_lock = threading.Lock()
        self.needed_workers = 0

    def stop(self):
        self._stopped = True
        if self.config:
            for z in self.config.rabbit_publishers.values():
                z.listener.stop()
                z.listener.join()
        self.manager.stop()
        self.manager.join()

    def loadConfig(self):
        self.log.debug("Loading configuration")
        config = yaml.load(open(self.configfile))

        newconfig = Config()
        newconfig.db = None
        newconfig.dburi = None
        newconfig.rabbit_publishers = {}
        newconfig.rabbit_queues = []
        newconfig.crons = {}

        for name, default in [
            ('cleanup', '* * * * *'),
            ('check', '*/15 * * * *'),
            ]:
            c = Cron()
            c.name = name
            newconfig.crons[c.name] = c
            c.job = None
            c.timespec = config.get('cron', {}).get(name, default)

        for addr in config['rabbit-publishers']:
            z = RabbitPublisher()
            z.name = addr
            z.listener = None
            newconfig.rabbit_publishers[z.name] = z

        for queue in config['rabbit-queues']:
            newconfig.rabbit_queues.append(queue)

        newconfig.languages = []
        for language in config['languages']:
            newconfig.languages.append(language)

        newconfig.dburi = config.get('dburi')
        newconfig.boot_timeout = config.get('boot-timeout')
        newconfig.launch_timeout = config.get('launch-timeout')
        newconfig.provider_token = config.get('provider-token')
        newconfig.provider_version = config.get('provider-version')
        newconfig.provider_size = config.get('provider-size')

        newconfig.private_user = config.get('private-user')
        newconfig.private_key = config.get('private-key')
        newconfig.messages_per_node = config.get('messages-per-node')

        return newconfig

    def reconfigureDatabase(self, config):
        if (not self.config) or config.dburi != self.config.dburi:
            config.db = nodedb.NodeDatabase(config.dburi)
        else:
            config.db = self.config.db

    def reconfigureUpdateListeners(self, config):
        for z in config.rabbit_publishers.values():
            self.log.debug("Starting listener for %s" % z.name)
            z.listener = RabbitListener(self, z.name)
            try:
                t = threading.Thread(target=z.listener.start).start()
            except KeyboardInterrupt:
                z.listener.stop()

    def setConfig(self, config):
        self.config = config

    def getDB(self):
        return self.config.db

    def getNeededWorkers(self):
        return self.needed_workers

    def setNeededWorkers(self, needed_workers):
        self.needed_workers = needed_workers

    def getNeededNodes(self, session):
        print "In get needed nodes"
        self.log.debug("Beginning node launch calculation")
        label_demand = self.getNeededWorkers()

        nodes = session.getNodes()

        def count_nodes(state):
            return len([n for n in nodes
                        if (n.state == state)])

        # Actual need is demand - (ready + building)
        start_demand = 0
        min_demand = 1 + label_demand
        n_ready = count_nodes(nodedb.READY)
        n_building = count_nodes(nodedb.BUILDING)
        n_test = count_nodes(nodedb.TEST)
        ready = n_ready + n_building + n_test
        demand = max(min_demand - ready, 0)
        self.log.debug("  Deficit: (start: %s min: %s ready: %s)" %
                           (start_demand, min_demand,
                            ready))

        return demand

    def updateConfig(self):
        config = self.loadConfig()
        self.reconfigureDatabase(config)
        self.setConfig(config)

        self.manager = manager.ScalatorManager(self)
        self.manager.start()        
        self.reconfigureUpdateListeners(config)

    def startup(self):
        self.updateConfig()

        with self.getDB().getSession() as session:
            for node in session.getNodes(state=nodedb.BUILDING):
                self.log.info("Setting building node id: %s to delete "
                              "on startup" % node.id)
                node.state = nodedb.DELETE

    def run(self):
        print "in run"
        try:
            self.startup()
        except Exception:
            self.log.exception("Exception in startup:")
        while not self._stopped:
            try:
                self.updateConfig()
                with self.getDB().getSession() as session:
                    self._run(session)
            except Exception:
                self.log.exception("Exception in main loop:")
            time.sleep(self.watermark_sleep)

    def _run(self, session):
        nodes_to_launch = self.getNeededNodes(session)

        self.log.info("Need to launch %s nodes" % nodes_to_launch)
        for i in range(nodes_to_launch):
            self.launchNode(session)

    def launchNode(self, session):
        try:
            self._launchNode(session)
        except Exception:
            self.log.exception(
                "Could not launch node")

    def _launchNode(self, session):
        node = session.createNode()
        timeout = self.config.boot_timeout
        launch_timeout = self.config.launch_timeout
        t = NodeLauncher(self, node.id, timeout, launch_timeout)
        t.start()

    def deleteNode(self, node_id):
        try:
            self._delete_threads_lock.acquire()
            if node_id in self._delete_threads:
                return
            t = NodeDeleter(self, node_id)
            self._delete_threads[node_id] = t
            t.start()
        except Exception:
            self.log.exception("Could not delete node %s", node_id)
        finally:
            self._delete_threads_lock.release()

    def _deleteNode(self, session, node):
        self.log.debug("Deleting node id: %s which has been in %s "
                       "state for %s hours" %
                       (node.id, nodedb.STATE_NAMES[node.state],
                        (time.time() - node.state_time) / (60 * 60)))
        # Delete a node
        if node.state != nodedb.DELETE:
            # Don't write to the session if not needed.
            node.state = nodedb.DELETE
        node.delete()
        self.log.info("Deleted node id: %s" % node.id)

    def _doPeriodicCheck(self):
        try:
            with self.getDB().getSession() as session:
                self.periodicCheck(session)
        except Exception:
            self.log.exception("Exception in periodic chack:")

    def periodicCheck(self, session):
        # This function should be run periodically to make sure we can
        # still access hosts via ssh.

        self.log.debug("Starting periodic check")
        for node in session.getNodes():
            if node.state != nodedb.READY:
                continue
            self.deleteNode(node.id)
        self.log.debug("Finished periodic check")

class ConfigValue(object):
    pass


class Config(ConfigValue):
    pass

class Cron(ConfigValue):
    pass

class RabbitPublisher(ConfigValue):
    pass