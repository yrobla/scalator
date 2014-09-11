import json
import logging
import random
import threading
import time
import yaml
import nodedb
import nodeutils as utils
import pika
import manager

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

class NodeUpdateListener(threading.Thread):
    log = logging.getLogger("scalator.NodeUpdateListener")

    def __init__(self, scalator, adr):
        threading.Thread.__init__(self, name='NodeUpdateListener')
        self.scalator = scalator
        parameters = pika.URLParameters(adr)
        pika_connection = pika.BlockingConnection(parameters)
        self.pika_channel = pika_connection.channel()
        self._stopped = False

    def run(self):
        while not self._stopped:
            try:
                method_frame, header_frame, body = self.pika_channel.basic_get()
                self.pika_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            except Exception:
                continue

            try:
                topic, data = body.split(None, 1)
                self.handleEvent(topic, data)
            except Exception:
                self.log.exception("Exception handling job:")

    def stop(self):
        self._stopped = True

    def handleEvent(self, topic, data):
        self.log.debug("Received: %s %s" % (topic, data))
        args = json.loads(data)
        node_name = args['node_name']
        if topic == 'onStarted':
            self.handleStartPhase(nodename)
        elif topic == 'onCompleted':
            pass
        elif topic == 'onFinalized':
            self.handleCompletePhase(nodename)
        elif topic == 'demand':
            self.scalator.needed_workers += 1
        else:
            raise Exception("Received job for unhandled phase: %s" %
                            topic)

    def handleStartPhase(self, nodename, jobname):
        with self.scalator.getDB().getSession() as session:
            node = session.getNodeByNodename(nodename)
            if not node:
                self.log.debug("Unable to find node with nodename: %s" %
                               nodename)
                return

            # Preserve the HOLD state even if a job starts on the node.
            if node.state != nodedb.HOLD:
                self.log.info("Setting node id: %s to USED" % node.id)
                node.state = nodedb.USED

    def handleCompletePhase(self, nodename):
        t = NodeCompleteThread(self.scalator, nodename)
        t.start()

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

    def launchNode(self, session):
        start_time = time.time()
        timestamp = int(start_time)

        hostname = 'host_%s' % str(self.node.id)
        self.node.hostname = hostname

        self.log.info("Creating server with hostname %s for node id: %s" % (hostname, self.node_id))
        server_id = self.scalator.manager.createServer(hostname)
        self.node.external_id = server_id
        session.commit()

        self.log.debug("Waiting for server %s for node id: %s" %
                       (server_id, self.node.id))
        server = self.scalator.manager.waitForServer(server_id, self.launch_timeout)
        if server['status'] != 'ACTIVE':
            raise LaunchStatusException("Server %s for node id: %s "
                                        "status: %s" %
                                        (server_id, self.node.id,
                                         server['status']))

        ip = server.get('public_v4')
        if not ip:
            raise LaunchNetworkException("Unable to find public IP of server")

        self.node.ip = ip
        self.log.debug("Node id: %s is running, ip: %s, testing ssh" %
                       (self.node.id, ip))
        connect_kwargs = dict(key_filename=self.image.private_key)

        if not utils.ssh_connect(ip, self.image.username,
                                 connect_kwargs=connect_kwargs,
                                 timeout=self.timeout):
            raise LaunchAuthException("Unable to connect via ssh")

        # Save the elapsed time for statsd
        dt = int((time.time() - start_time) * 1000)
        self.node.state = nodedb.READY
        self.log.info("Node id: %s is ready" % self.node.id)

        return dt

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
        self.manager = manager.ScalatorManager()

    def stop(self):
        self._stopped = True
        if self.config:
            for z in self.config.rabbit_publishers.values():
                z.listener.stop()
                z.listener.join()

    def loadConfig(self):
        self.log.debug("Loading configuration")
        config = yaml.load(open(self.configfile))

        newconfig = Config()
        newconfig.db = None
        newconfig.dburi = None
        newconfig.rabbit_publishers = {}
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

        newconfig.dburi = config.get('dburi')
        newconfig.boot_timeout = config.get('boot-timeout')
        newconfig.launch_timeout = config.get('launch-timeout')
        return newconfig

    def reconfigureDatabase(self, config):
        if (not self.config) or config.dburi != self.config.dburi:
            config.db = nodedb.NodeDatabase(config.dburi)
        else:
            config.db = self.config.db

    def reconfigureUpdateListeners(self, config):
        if self.config:
            running = set(self.config.rabbit_publishers.keys())
        else:
            running = set()

        configured = set(config.rabbit_publishers.keys())
        if running == configured:
            self.log.debug("Rabbit Listeners do not need to be updated")
            config.rabbit_publishers = self.config.rabbit_publishers
            return

        for z in config.rabbit_publishers.values():
            self.log.debug("Starting listener for %s" % z.name)
            z.listener = NodeUpdateListener(self, z.name)
            z.listener.start()

    def setConfig(self, config):
        self.config = config

    def getDB(self):
        return self.config.db

    def getNeededWorkers(self):
        return self.needed_workers

    def getNeededNodes(self, session):
        self.log.debug("Beginning node launch calculation")
        label_demand = self.getNeededWorkers()

        nodes = session.getNodes()

        def count_nodes(state):
            return len([n for n in nodes
                        if (n.state == state)])

        # Actual need is demand - (ready + building)
        start_demand = 0
        min_demand = 1
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
        self.reconfigureUpdateListeners(config)
        self.setConfig(config)

    def startup(self):
        self.updateConfig()

        with self.getDB().getSession() as session:
            for node in session.getNodes(state=nodedb.BUILDING):
                self.log.info("Setting building node id: %s to delete "
                              "on startup" % node.id)
                node.state = nodedb.DELETE

    def run(self):
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
