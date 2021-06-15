import logging
from operator import add
from scrap_chord.util import in_between
import sys
import pickle
from hashlib import blake2b, sha1
from threading import Lock, Thread
from typing import Tuple
from utils.const import CHORD_BEACON_PORT, CODE_WORD_CHORD, HASH_SEED

import zmq.sugar as zmq
from sortedcontainers import SortedSet

from ..utils.tools import connect_router, find_nodes, get_id, get_router, get_source_ip, parse_address, register_socks, zpipe


class ScrapChordClient:
    def __init__(self, port, m) -> None:
        self.address = (get_source_ip(), port)
        self.context = zmq.Context()
        self.usr_send_pipe = zpipe(self.context)
        self.recv_send_pipe = zpipe(self.context)

        self.bits = m
        self.recv_cache = dict()
        self.online = True

        # debbug & info
        logging.basicConfig(format = "client: %(levelname)s: %(message)s", level=logging.INFO)
        self.logger = logging.getLogger("scrapper")

    def run(self, address:str = ""):
        self.logger.info(f"Running on {self.address[0]}:{self.address[1]}")

        known_nodes = SortedSet(key = lambda n: n[0])
        if address == "":
            self.logger.info("No adress specified, finding chords online ...")
            self.add_node(known_nodes, self.get_chord_nodes())
            if len(known_nodes) == 0:
                self.logger.info("No available chord nodes found. Exiting ...")
                return
        else:
            self.add_node(parse_address(address))
        
        t = Thread(target=self.communicate_with_chord, args=(known_nodes,), daemon=True)
        t.start()
        
        poller = zmq.Poller()
        register_socks(poller, self.usr_send_pipe[0], sys.stdin)
        while self.online:
            socks = dict(poller.poll())
            if self.usr_send_pipe[0] in socks:
                url, html = self.usr_send_pipe[0].recv_pyobj(zmq.NOBLOCK)
                self.logger.info(f"Recieved {url}\n {html[:100]} \n ...") # Print url and first 100 chars from html
            if sys.stdin.fileno() in socks:
                for line in sys.stdin:
                    self.usr_send_pipe[0].send_pyobj(line.split())
                    break

    def communicate_with_chord(self, known_nodes:SortedSet):
        comm_sock = get_router(self.context)
        
        pending_recv = SortedSet()
        poller = zmq.Poller()
        register_socks(
            poller, self.recv_send_pipe[1], self.usr_send_pipe[1]
        )
        while True:
            socks = dict(poller.poll(1500))
            if self.usr_send_pipe[1] in socks:
                url_list:Tuple = self.recv_send_pipe[1].recv_pyobj()
            elif comm_sock in socks:
                _, node_addr_bytes, message = comm_sock.recv_multipart()
                self.add_node(known_nodes, pickle.loads(node_addr_bytes))
                url, html, url_list = pickle.loads(message)
                self.recv_cache[url] = html
                self.usr_send_pipe[1].send_pyobj((url, html)) # Send recieved url and html to main thread for display
                pending_recv.remove(url)
            else:
                url_list = pending_recv

            for url in url_list:
                if url in self.recv_cache:
                    continue
                pending_recv.add(url)
                target_addr = self.select_target_node(url, known_nodes)
                message = pickle.dumps((url, self.address))
                comm_sock.send_multipart([target_addr.encode(), message])

    def select_target_node(self, url, known_nodes) -> str:
        if len(known_nodes) == 0:
            raise Exception("There are no known nodes")
        if len(known_nodes) == 1:
            return known_nodes[0][1]
        url_id = get_id(url) % (2 ** self.bits)
        lwb_id, _ = known_nodes[-1]
        for node_id, addr in known_nodes:
            if in_between(self.bits, url_id, lwb_id, node_id):
                return addr
        
        raise Exception("A node must be always found")

    def add_node(self, known_nodes:SortedSet, *nodes_addr):
        for addr in known_nodes:
            addr_hash = f"{addr[0]}:{addr[1]}"
            idx = get_id(addr_hash) % (2 ** self.bits)
            known_nodes.add(idx, addr)

    def get_chord_nodes(self):
        chord_nodes = find_nodes(
            port=CHORD_BEACON_PORT,
            code_word=CODE_WORD_CHORD,
            tolerance=3,
            all = 1
        )
        return chord_nodes