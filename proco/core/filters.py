import logging
import socket


class HostInfoFilter(logging.Filter):

    def filter(self, record):
        record.hostname = socket.gethostname()
        record.hostip = socket.gethostbyname(record.hostname)

        return True
