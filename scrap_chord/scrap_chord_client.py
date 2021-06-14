import logging
import sys
from typing import Tuple
import zmq.sugar as zmq
from sortedcontainers import SortedSet
from ..utils.tools import get_source_ip, zpipe


class ScrapChordClient:
    def __init__(self, port) -> None:
        self.address = (get_source_ip(), port)
        self.context = zmq.Context()
        self.usr_recv_pipe = zpipe(self.context)
        self.usr_send_pipe = zpipe(self.context)

        self.pending_recv = SortedSet()
        self.recv = dict()

        # debbug & info
        logging.basicConfig(format = "client: %(levelname)s: %(message)s", level=logging.INFO)
        self.logger = logging.getLogger("scrapper")

    def run(self):
        self.logger.info(f"Running on {self.address[0]}:{self.address[1]}")
       
        poller = zmq.Poller()
        poller.register(self.usr_recv_pipe[0], zmq.POLLIN)
        poller.register(sys.stdin.fileno(), zmq.POLLIN)

        while True:
            socks = dict(poller.poll())
            if self.usr_recv_pipe[0] in socks:
                pass
            else: 
                pass

    def send_work(self):
        send_sock = self.context.socket(zmq.ROUTER)
        # TODO: Connect send_sock to someone

        while True:
            obj:Tuple = self.usr_send_pipe[1].recv_pyobj()
            for url in obj:
                if url in self.recv:
                    continue
                self.pending_recv.add(url)
                send_sock.send_multipart()
    
    def recv_work(self):
        recv_sock = self.context.socket(zmq.ROUTER)
        recv_sock.bind(f"tcp://{self.address[0]}:{self.address[1]}")
        while True:
            obj = recv_sock.recv_pyobj()
            self.usr_recv_pipe[1].send_pyobj(obj)