import argparse
import daemon
import errno
import extras

pid_file_module = extras.try_imports(['daemon.pidlockfile', 'daemon.pidfile'])

import logging.config
import os
import sys
import signal
import traceback
import threading

def stack_dump_handler(signum, frame):
    signal.signal(signal.SIGUSR2, signal.SIG_IGN)
    log_str = ""
    threads = {}
    for t in threading.enumerate():
        threads[t.ident] = t
    for thread_id, stack_frame in sys._current_frames().items():
        thread = threads.get(thread_id)
        if thread:
            thread_name = thread.name
        else:
            thread_name = 'Unknown'
        label = '%s (%s)' % (thread_name, thread_id)
        log_str += "Thread: %s\n" % label
        log_str += "".join(traceback.format_stack(stack_frame))
    log = logging.getLogger("scalator.stack_dump")
    log.debug(log_str)
    signal.signal(signal.SIGUSR2, stack_dump_handler)

def is_pidfile_stale(pidfile):
    """ Determine whether a PID file is stale.

        Return 'True' ("stale") if the contents of the PID file are
        valid but do not match the PID of a currently-running process;
        otherwise return 'False'.

        """
    result = False
    pidfile_pid = pidfile.read_pid()
    if pidfile_pid is not None:
        try:
            os.kill(pidfile_pid, 0)
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                # The specified PID does not exist
                result = True

    return result

class ScalatorDaemon(object):
    def __init__(self):
        self.args = None

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description='Scalator')
        parser.add_argument('-c', dest='config',
                            default='/etc/scalator/scalator.yaml',
                            help='path to config file')
        parser.add_argument('-d', dest='nodaemon', action='store_true',
                            help='do not run as a daemon')
        parser.add_argument('-l', dest='logconfig',
                            help='path to log config file',
                            default='/etc/scalator/logging.conf')
        parser.add_argument('-p', dest='pidfile',
                            help='path to pid file',
                            default='/var/run/scalator/scalator.pid')
        self.args = parser.parse_args()

    def setup_logging(self):
        if self.args.logconfig:
            fp = os.path.expanduser(self.args.logconfig)
            if not os.path.exists(fp):
                raise Exception("Unable to read logging config file at %s" %
                                fp)
            logging.config.fileConfig(fp)
        else:
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(levelname)s %(name)s: '
                                       '%(message)s')

    def exit_handler(self, signum, frame):
        self.scalator.stop()

    def term_handler(self, signum, frame):
        os._exit(0)

    def main(self):
        import scalator
        self.setup_logging()
        self.scalator = scalator.Scalator(self.args.config)

        signal.signal(signal.SIGUSR1, self.exit_handler)
        signal.signal(signal.SIGUSR2, stack_dump_handler)
        signal.signal(signal.SIGTERM, self.term_handler)

        self.scalator.start()

        while True:
            try:
                signal.pause()
            except KeyboardInterrupt:
                return self.exit_handler(signal.SIGINT, None)


def main():
    sd = ScalatorDaemon()
    sd.parse_arguments()

    pid = pid_file_module.TimeoutPIDLockFile(sd.args.pidfile, 10)
    if is_pidfile_stale(pid):
        pid.break_lock()

    if sd.args.nodaemon:
        sd.main()
    else:
        with daemon.DaemonContext(pidfile=pid):
            sd.main()


if __name__ == "__main__":
    sys.exit(main())

