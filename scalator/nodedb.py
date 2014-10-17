import time

from sqlalchemy import Table, Column, Integer, String, Text, \
    MetaData, create_engine, or_
from sqlalchemy.orm import scoped_session, mapper, relationship, foreign
from sqlalchemy.orm.session import Session, sessionmaker

# not ready for use.
BUILDING = 1
# The machine is ready for use.
READY = 2
# This can mean in-use, or used but complete.
USED = 3
# Delete this machine immediately.
DELETE = 4
# Keep this machine indefinitely.
HOLD = 5
# Acceptance testing (pre-ready)
TEST = 6

STATE_NAMES = {
    BUILDING: 'building',
    READY: 'ready',
    USED: 'used',
    DELETE: 'delete',
    HOLD: 'hold',
    TEST: 'test',
    }

metadata = MetaData()

node_table = Table(
    'node', metadata,
    Column('id', Integer, primary_key=True),
    # Machine name
    Column('hostname', String(255), index=True),
    # node name
    Column('nodename', String(255), index=True),
    # Provider assigned id for this machine
    Column('external_id', String(255)),
    # Primary IP address
    Column('ip', String(255)),
    # One of the above values
    Column('state', Integer),
    # Time of last state change
    Column('state_time', Integer),
    )

class Node(object):
    def __init__(self, hostname=None, external_id=None, ip=None,
                 state=BUILDING):
        self.external_id = external_id
        self.ip = ip
        self.hostname = hostname
        self.state = state

    def delete(self):
        session = Session.object_session(self)
        session.delete(self)
        session.commit()

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state
        self.state_time = int(time.time())
        session = Session.object_session(self)
        if session:
            session.commit()

mapper(Node, node_table,
       properties=dict(
           _state=node_table.c.state))

class NodeDatabase(object):
    def __init__(self, dburi):
        engine_kwargs = dict(echo=False, pool_recycle=3600)
        if 'sqlite:' not in dburi:
            engine_kwargs['max_overflow'] = -1

        self.engine = create_engine(dburi, **engine_kwargs)
        metadata.create_all(self.engine)
        self.session_factory = sessionmaker(bind=self.engine)
        self.session = scoped_session(self.session_factory)

    def getSession(self):
        return NodeDatabaseSession(self.session)

class NodeDatabaseSession(object):
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self

    def __exit__(self, etype, value, tb):
        if etype:
            self.session().rollback()
        else:
            self.session().commit()
        self.session().close()
        self.session = None

    def abort(self):
        self.session().rollback()

    def commit(self):
        self.session().commit()

    def delete(self, obj):
        self.session().delete(obj)

    def getNodes(self, state=None):
        exp = self.session().query(Node)
        if state:
            exp = exp.filter(node_table.c.state == state)
        return exp.all()

    def createNode(self, *args, **kwargs):
        new = Node(*args, **kwargs)
        self.session().add(new)
        self.commit()
        return new

    def getNode(self, id):
        nodes = self.session().query(Node).filter_by(id=id).all()
        if not nodes:
            return None
        return nodes[0]

    def getNodeByHostname(self, hostname):
        nodes = self.session().query(Node).filter_by(hostname=hostname).all()
        if not nodes:
            return None
        return nodes[0]

    def getNodeByNodename(self, nodename):
        nodes = self.session().query(Node).filter_by(nodename=nodename).all()
        if not nodes:
            return None
        return nodes[0]

    def getNodeByExternalID(self, external_id):
        nodes = self.session().query(Node).all()
        nodes = self.session().query(Node).filter_by(
            external_id=external_id).all()
        if not nodes:
            return None
        return nodes[0]

    def getNodesToDelete(self, limit):
        # get the ones in building state, from newer to older
        # and the ones in ready state, from newer to older as well
        nodes = self.session().query(Node).filter(
            ( node_table.c.state == BUILDING ) | 
              (node_table.c.state == READY)).order_by(
            node_table.c.state, node_table.c.state_time).limit(limit).all()
        return nodes


