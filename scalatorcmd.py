import argparse
import logging.config
import sys
import time

import nodedb
import scalator
import manager
from prettytable import PrettyTable

class ScalatorCmd(object):
    def __init__(self):
        self.args = None

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='Scalator')
        parser.add_argument('-c', dest='config',
                            default='/etc/scalator/scalator.yaml',
                            help='path to config file')
        parser.add_argument('--version', dest='version', action='store_true',
                            help='show version')
        parser.add_argument('--debug', dest='debug', action='store_true',
                            help='show DEBUG level logging')

        subparsers = parser.add_subparsers(title='commands',
                                           description='valid commands',
                                           help='additional help')

        cmd_list = subparsers.add_parser('list', help='list nodes')
        cmd_list.set_defaults(func=self.list)

        cmd_alien_list = subparsers.add_parser(
            'alien-list',
            help='list nodes not accounted for by scalator')
        cmd_alien_list.set_defaults(func=self.alien_list)

        cmd_delete = subparsers.add_parser(
            'delete',
            help='place a node in the DELETE state')
        cmd_delete.set_defaults(func=self.delete)
        cmd_delete.add_argument('id', help='node id')

        self.args = parser.parse_args()

    def setup_logging(self):
        if self.args.debug:
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(levelname)s %(name)s: '
                                       '%(message)s')
        else:
            logging.basicConfig(level=logging.INFO,
                                format='%(asctime)s %(levelname)s %(name)s: '
                                       '%(message)s')

    def list(self, node_id=None):
        t = PrettyTable(["ID", "Hostname",
                         "NodeName", "Server ID", "IP", "State",
                         "Age (hours)"])
        t.align = 'l'
        now = time.time()
        with self.scalator.getDB().getSession() as session:
            for node in session.getNodes():
                if node_id and node.id != node_id:
                    continue
                t.add_row([node.id, node.hostname,
                           node.nodename, node.external_id, node.ip,
                           nodedb.STATE_NAMES[node.state],
                           '%.02f' % ((now - node.state_time) / 3600)])
        print t

    def alien_list(self):
        t = PrettyTable(["Hostname", "Server ID", "IP"])
        t.align = 'l'
        with self.scalator.getDB().getSession() as session:
                scalator_manager = manager.ScalatorManager(self.scalator)
                scalator_manager.start()
                for server in scalator_manager.listServers():
                    print server['id']
                    print session.getNodeByExternalID(server['id'])
                    if not session.getNodeByExternalID(server['id']):
                        t.add_row([server['name'], server['id'],
                                   server['public_v4']])
                scalator_manager.stop()
        print t

    def delete(self):
        with self.scalator.getDB().getSession() as session:
            node = session.getNode(self.args.id)

            self.scalator.manager = manager.ScalatorManager(self.scalator)
            self.scalator.manager.start()
            self.scalator._deleteNode(session, node)
            self.scalator.manager.stop()

    def main(self):
        self.scalator = scalator.Scalator(self.args.config)
        config = self.scalator.loadConfig()
        self.scalator.reconfigureDatabase(config)
        self.scalator.setConfig(config)
        self.args.func()

def main():
    npc = ScalatorCmd()
    npc.parse_arguments()
    npc.setup_logging()
    return npc.main()

if __name__ == "__main__":
    sys.exit(main())

