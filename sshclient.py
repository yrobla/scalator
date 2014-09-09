import paramiko


class SSHClient(object):
    def __init__(self, ip, username, password=None, pkey=None,
                 key_filename=None, log=None):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
        client.connect(ip, username=username, password=password, pkey=pkey,
                       key_filename=key_filename)
        self.client = client
        self.log = log

    def ssh(self, action, command, get_pty=True, output=False):
        if self.log:
            self.log.info(command)
        stdin, stdout, stderr = self.client.exec_command(
            command, get_pty=get_pty)
        out = ''
        err = ''
        for line in stdout:
            if output:
                out += line
            if self.log:
                self.log.info(line.rstrip())
        for line in stderr:
            if output:
                err += line
            if self.log:
                self.log.error(line.rstrip())
        ret = stdout.channel.recv_exit_status()
        if ret:
            raise Exception(
                "Unable to %s\ncommand: %s\nstdout: %s\nstderr: %s"
                % (action, command, out, err))
        return out

    def scp(self, source, dest):
        if self.log:
            self.log.info("Copy %s -> %s" % (source, dest))
        ftp = self.client.open_sftp()
        ftp.put(source, dest)
        ftp.close()

