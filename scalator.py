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

class NodeUpdateListener():
    logger = logging.getLogger("scalator.NodeUpdateListener")

    EXCHANGE = 'message'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'scalator'
    ROUTING_KEY = 'node_status'

    def __init__(self, scalator, adr):
        self.scalator = scalator
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = adr

    def connect(self):
        self.logger.debug('Connecting to rabbit %s' % self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        self.logger.debug('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        self.logger.debug('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.logger.debug('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        self.logger.debug('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        self.logger.debug('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        self.logger.debug('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        self.logger.debug('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        self.logger.debug('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        self.logger.debug('Exchange declared')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self.logger.debug('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        self.logger.debug('Binding %s to %s with %s',
                    self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE, self.ROUTING_KEY)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self.logger.debug('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        self.logger.debug('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        self.logger.debug('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def on_message(self, unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """

        # increase demand
        self.scalator.increaseNeededNodes()

        # ack message
        self.logger.debug('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        self.acknowledge_message(basic_deliver.delivery_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        self.logger.debug('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            self.logger.debug('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        self.logger.debug('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.QUEUE)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        self.logger.debug('Queue bound')
        self.start_consuming()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        self.logger.debug('Closing the channel')
        self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        self.logger.debug('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        print "before connect"
        self._connection = self.connect()
        print "before start"
        self._connection.ioloop.start()
        print "after start"

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self.logger.debug('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        self.logger.debug('Stopped')
	
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
                print "in launch node"
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

        hostname = 'host-%s' % str(self.node.id)
        self.node.hostname = hostname

        self.log.info("Creating server with hostname %s for node id: %s" % (hostname, self.node_id))
        print "In create server"
        server = self.scalator.manager.createServer(hostname)
        self.node.external_id = server.get('id')
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

        # Save the elapsed time for statsd
        dt = int((time.time() - start_time) * 1000)
        self.node.state = nodedb.READY
        self.log.info("Node id: %s is ready" % self.node.id)

        return dt

class Scalator(threading.Thread):
    log = logging.getLogger("scalator.Scalator")

    def __init__(self, configfile, watermark_sleep=WATERMARK_SLEEP):
        print "in init thread"
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
        newconfig.provider_token = config.get('provider-token')
        newconfig.provider_version = config.get('provider-version')
        newconfig.provider_size = config.get('provider-size')

        newconfig.private_user = config.get('private-user')
        newconfig.private_key = config.get('private-key')
        return newconfig

    def increaseNeededNodes(self):
        # lock var, update it and unlock
        lock = threading.Lock()
        lock.acquire()
        self.needed_workers += 1
        lock.release()

    def decreaseNeededNodes(self):
        # lock var, update it and unlock
        lock = threading.Lock()
        lock.acquire()
        self.needed_workers += 1
        lock.release()

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
            try:
                threading.Thread(target=z.listener.run).start()
            except KeyboardInterrupt:
                z.listener.stop()

    def setConfig(self, config):
        self.config = config

    def getDB(self):
        return self.config.db

    def getNeededWorkers(self):
        return self.needed_workers

    def getNeededNodes(self, session):
        print "in get needed nodes"
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
        self.reconfigureUpdateListeners(config)
        self.setConfig(config)
        self.manager = manager.ScalatorManager(self)
        self.manager.start()        

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
