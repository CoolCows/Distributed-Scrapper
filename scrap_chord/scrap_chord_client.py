import logging
import sys
import pickle
from threading import Lock, Thread
from typing import Tuple

import zmq.sugar as zmq
from sortedcontainers import SortedSet

from ..utils.tools import connect_router, get_router, get_source_ip, register_socks, zpipe


class ScrapChordClient:
    def __init__(self, port) -> None:
        self.address = (get_source_ip(), port)
        self.context = zmq.Context()
        self.usr_send_pipe = zpipe(self.context)
        self.recv_send_pipe = zpipe(self.context)

        self.pending_recv = SortedSet()
        self.recv = dict()

        self.known_chords = []

        # sync
        self.recv_url_lock = Lock()

        # debbug & info
        logging.basicConfig(format = "client: %(levelname)s: %(message)s", level=logging.INFO)
        self.logger = logging.getLogger("scrapper")

    def run(self):
        self.logger.info(f"Running on {self.address[0]}:{self.address[1]}")
       
        poller = zmq.Poller()
        register_socks(poller, self.usr_send_pipe[0], sys.stdin.fileno())
        while True:
            socks = dict(poller.poll())
            if self.usr_send_pipe[0] in socks:
                pass
            else: 
                pass

    def communicate_with_chord(self):
        comm_sock = get_router(self.context)
        connect_router(comm_sock, "an address here")
        # TODO: Connect send_sock to someone

        recv_sock = self.context.socket(zmq.DEALER)
        recv_sock.bind(f"tcp://{self.address[0]}:{self.address[1]}")

        poller = zmq.Poller()
        register_socks(
            poller, recv_sock, self.recv_send_pipe[1], self.usr_send_pipe[1]
        )
        while True:
            socks = dict(poller.poll(1500))
            if self.usr_send_pipe[1] in socks:
                url_list:Tuple = self.recv_send_pipe[1].recv_pyobj()
            elif recv_sock in socks:
                message = recv_sock.recv_multipart()[0]
                url, html, url_list = pickle.loads(message)
                self.recv[url] = html
                self.usr_send_pipe[1].send_pyobj((url, html)) # Send Message to main thread about recieved url
                self.pending_recv.remove(url)
            else:
                url_list = self.pending_recv

            self.recv_url_lock.acquire()
            for url in url_list:
                if url in self.recv:
                    continue
                self.pending_recv.add(url)
                # TODO: Address to send
                message = pickle.dumps((url, self.address))
                comm_sock.send_multipart([b"objective address", message])
            self.recv_url_lock.release()
