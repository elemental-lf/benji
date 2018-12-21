#!/usr/bin/env python3
from socketserver import BaseRequestHandler,UnixDatagramServer 
from os import unlink
import sys

class SyslogHandler(BaseRequestHandler):

    def handle(self):
        data = bytes.decode(self.request[0], encoding='utf-8')
        print(data, flush=True)
		
if __name__ == "__main__":

    try:
        unlink('/dev/log')
    except FileNotFoundError:
        pass
    server = UnixDatagramServer('/dev/log', SyslogHandler)
    server.serve_forever(poll_interval=0.5)
