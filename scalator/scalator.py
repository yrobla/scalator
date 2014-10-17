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
import apscheduler.scheduler
from jinja2 import Environment, FileSystemLoader
import statsd

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

        if int(node.state) == nodedb.HOLD:
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
                self.log.debug("in  node delete %s" % self.node_id)
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
                self.log.debug("in run node")
                start_time = time.time()
                dt = self.launchNode(session)
                failed = False
                statsd_key = 'scalator.launch.ready'
            except Exception as e:
                self.log.debug("fail")
                self.log.debug(str(e))
                self.log.exception("%s launching node id: %s "
                                   "error:" %
                                   (e.__class__.__name__,
                                    self.node_id))
                dt = int((time.time() - start_time) * 1000)
                failed = True

            try:
                statsd_key = 'scalator.launch.ready'
                self.scalator.launchStats(statsd_key, dt)
            except Exception:
                self.log.exception("Exception reporting launch stats")

            if failed:
                self.log.debug("i delete node")
                try:
                    self.scalator._forceDeleteNode(session, self.node)
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

        # render template and copy to config file
        env = Environment(loader=FileSystemLoader('/var/lib/scalator/templates'))
        template = env.get_template('rabbit_config')

        result = template.render(languages=','.join(self.scalator.config.languages),
                                 host=self.scalator.config.rabbit_host,
                                 username=self.scalator.config.rabbit_user,
                                 password=self.scalator.config.rabbit_password)
        host.ssh("copy result to file",
                 "cat <<EOF > /root/rabbit_config\n"
                 "%s\n"
                 "EOF" % result)

    def launchNode(self, session):
        start_time = time.time()
        timestamp = int(start_time)

        hostname = 'host-%s' % str(self.node.id)
        self.node.hostname = hostname

        self.log.info("Creating server with hostname %s for node id: %s" % (hostname, self.node_id))
        server = self.scalator.manager.createServer(hostname)
        try:
            self.node.external_id = server.get('id')
            self.node.nodename = server.get('name')
            session.commit()
        except Exception as e:
            self.log.debug("Error updating node: %s" % str(e))

        if server:
            ip = server.get('public_v4')
            if not ip:
                raise LaunchNetworkException("Unable to find public IP of server")
        else:
            raise Exception("Unable to create server")        

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
        self.log.debug("in read pika")
        total_messages = 0
        self.connection = pika.BlockingConnection(pika.URLParameters(self.addr))
        for queue in self.scalator.config.rabbit_queues:
            if self.connection:
                channel = self.connection.channel(None)
                q = channel.queue_declare(queue=queue, passive=True, durable=True, exclusive=False)
                total_messages += q.method.message_count

        self.log.debug("in total messages")
        self.scalator.setNeededWorkers(int(math.ceil(total_messages/self.scalator.config.messages_per_node)))
        self.log.debug(total_messages)
        self.log.debug("here")
        self.scalator.updateStats(total_messages)

        self.connection = None
        time.sleep(WATERMARK_SLEEP)


class Scalator(threading.Thread):
    log = logging.getLogger("scalator.Scalator")
    logging.getLogger('pika').setLevel(logging.INFO)

    def __init__(self, configfile, watermark_sleep=WATERMARK_SLEEP):
        threading.Thread.__init__(self, name='Scalator')
        self.configfile = configfile
        self.watermark_sleep = watermark_sleep
        self._stopped = False
        self.config = None
        self._delete_threads = {}
        self._delete_threads_lock = threading.Lock()
        self.needed_workers = None
        self.apsched = None

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
        newconfig.max_servers = config.get('max-servers')

        newconfig.rabbit_host = config.get('rabbit-host')
        newconfig.rabbit_user = config.get('rabbit-user')
        newconfig.rabbit_password = config.get('rabbit-password')

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
        self.log.debug("Beginning node launch calculation")
        label_demand = self.getNeededWorkers()

        # if no workers set, demand still not calculated
        if label_demand is None:
            return 0

        nodes = session.getNodes()

        def count_nodes(state=None):
            if state is not None:
                return len([n for n in nodes
                            if (n.state == state)])
            else:
                return len(nodes)

        # Actual need is demand - (ready + building)
        start_demand = 0
        min_demand = 1 + label_demand
        n_ready = count_nodes(nodedb.READY)
        n_building = count_nodes(nodedb.BUILDING)
        n_test = count_nodes(nodedb.TEST)
        ready = n_ready + n_building + n_test

        if min_demand < ready:
            # we may need to delete nodes
            return min_demand - ready
        else:
            demand = max(min_demand - ready, 0)

            n_provider = count_nodes()
            available = self.config.max_servers - n_provider
            if available < 0:
                self.log.warning("Provider over-allocated: limit %d, allocated %d" %
                    (self.config.max_servers, n_provider))
            demand = min(available, demand)
            self.log.debug("  Deficit: (start: %s min: %s ready: %s)" %
                               (start_demand, min_demand,
                               ready))

        return demand

    def cleanupOneNode(self, session, node):
        now = time.time()
        self.log.debug("in cleanup")
        self.log.debug("node id %s - state %s - ip %s" % (str(node.id), str(node.state), str(node.ip)))
        time_in_state = now - node.state_time
        if (int(node.state) in [nodedb.READY, nodedb.HOLD]):
            return
        delete = False
        self.log.debug("need to delete")
        if (int(node.state) == nodedb.DELETE):
            delete = True
        elif (int(node.state) == nodedb.TEST and
              time_in_state > TEST_CLEANUP):
            delete = True
        elif time_in_state > NODE_CLEANUP:
            self.log.debug("more than timeout")
            delete = True
        if delete:
            self.log.debug("delete node %s" % node.id)
            try:
                self.deleteNode(node.id)
            except Exception:
                self.log.exception("Exception deleting node id: "
                                   "%s" % node.id)

    def periodicCleanup(self):
        # This function should be run periodically to clean up any hosts
        # that may have slipped through the cracks, as well as to remove
        # old images.

        self.log.debug("Starting periodic cleanup")

        for k, t in self._delete_threads.items()[:]:
            if not t.isAlive():
                del self._delete_threads[k]

        node_ids = []
        with self.getDB().getSession() as session:
            for node in session.getNodes():
                node_ids.append(node.id)

        self.log.debug("in periodic cleanup")
        for node_id in node_ids:
            try:
                with self.getDB().getSession() as session:
                    node = session.getNode(node_id)
                    if node:
                        self.log.debug("in cleanup %s" % str(node_id))
                        self.cleanupOneNode(session, node)
            except Exception:
                self.log.exception("Exception cleaning up node id %s:" %
                                   node_id)
        self.log.debug("Finished periodic cleanup")

    def _doPeriodicCleanup(self):
        try:
            self.periodicCleanup()
        except Exception:
            self.log.exception("Exception in periodic cleanup:")

    def reconfigureCrons(self, config):
        cron_map = {
            'cleanup': self._doPeriodicCleanup
        }

        if not self.apsched:
            self.apsched = apscheduler.scheduler.Scheduler()
            self.apsched.start()

        for c in config.crons.values():
            if ((not self.config) or
                c.timespec != self.config.crons[c.name].timespec):
                if self.config and self.config.crons[c.name].job:
                    self.apsched.unschedule_job(self.config.crons[c.name].job)
                parts = c.timespec.split()
                minute, hour, dom, month, dow = parts[:5]
                c.job = self.apsched.add_cron_job(
                    cron_map[c.name],
                    day=dom,
                    day_of_week=dow,
                    hour=hour,
                    minute=minute)
            else:
                c.job = self.config.crons[c.name].job

    def updateConfig(self):
        config = self.loadConfig()
        self.reconfigureDatabase(config)
        self.reconfigureCrons(config)
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

    # delete nodes that are not used
    def deleteNodes(self, number_of_nodes):
        self.log.debug("in delete nodes")
        nodes_to_delete = self.getDB().getSession().getNodesToDelete(number_of_nodes)
        self.log.debug("need to delete")
        self.log.debug(nodes_to_delete)
        for node in nodes_to_delete:
            self.deleteNode(node.id)

    def _run(self, session):
        nodes_to_launch = self.getNeededNodes(session)
        if nodes_to_launch < 0:
            # need to delete nodes
            self.deleteNodes(-nodes_to_launch)
        else:
            if nodes_to_launch>self.config.max_servers:
                nodes_to_launch = self.config.max_servers

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
            self.log.debug("I need to delete %s" % node_id)
            t = NodeDeleter(self, node_id)
            self._delete_threads[node_id] = t
            t.start()
        except Exception:
            self.log.exception("Could not delete node %s", node_id)
        finally:
            self._delete_threads_lock.release()

    def _deleteNode(self, session, node):
        self.log.debug("in _delete node %s" % node.id)
        if node.state_time:
            self.log.debug("Deleting node id: %s which has been in %s "
                           "state for %s hours" %
                           (node.id, nodedb.STATE_NAMES[int(node.state)],
                           (time.time() - node.state_time) / (60 * 60)))
        # Delete a node
        if int(node.state) != nodedb.DELETE:
            # Don't write to the session if not needed.
            node.state = nodedb.DELETE

        # check if we need to delete it on the manager
        if node.external_id:
            self.log.debug('Deleting server %s for node id %s' %
                (node.external_id, node.id))
            self.manager.cleanupServer(node.nodename, node.ip)
            node.external_id = None

        node.delete()
        self.log.info("Deleted node id: %s" % node.id)

    def _forceDeleteNode(self, session, node):
        # Forcing to delete a node
        if node:
            try:
                current_manager = manager.ScalatorManager(self)
                server = current_manager._client.Droplet.get(node.nodename)
                server.destroy()
            except Exception as e:
                self.log.debug(str(e))
            node.delete()
            self.log.info("Forced deletion of node id: %s" % node.id)

    def updateStats(self, number):
        base_key = 'scalator.messages'

        c = statsd.StatsClient('localhost', 8125)
        c.gauge(base_key, number)

    def launchStats(self, key, dt):
        c = statsd.StatsClient('localhost', 8125)
        c.timing(key, dt)
        c.incr(key)


class ConfigValue(object):
    pass


class Config(ConfigValue):
    pass

class Cron(ConfigValue):
    pass

class RabbitPublisher(ConfigValue):
    pass
