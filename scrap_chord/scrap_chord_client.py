import logging
import time
from requests.api import request
from scraper.scraper_const import MAX_IDDLE, TIMEOUT_COMM
from scrap_chord.util import add_search_tree, in_between, parse_requests, remove_back_slashes, reset_times, select_target_node, update_search_trees
import sys
import pickle
from threading import  Thread
from typing import Tuple
from utils.const import CHORD_BEACON_PORT, CODE_WORD_CHORD, REP_CLIENT_INFO, REP_CLIENT_NODE

import zmq.sugar as zmq
from sortedcontainers import SortedSet

from utils.tools import address_to_string, connect_router, find_nodes, get_id, get_router, get_source_ip, parse_address, register_socks, zpipe


class ScrapChordClient:
    def __init__(self, port, m, gui_sock:zmq.Socket = None) -> None:
        self.address = (get_source_ip(), port)
        
        self.context = zmq.Context()
        self.usr_send_pipe = zpipe(self.context)
        self.gui_sock = gui_sock

        self.bits = m
        self.local_cache = dict()
        self.online = True
        
        # debbug & info
        logging.basicConfig(
            format = "%(name)s: %(levelname)s: %(message)s", level=logging.DEBUG
        )
        self.logger = logging.getLogger("client")

    def run(self, address:str = ""):
        self.logger.info(f"Client running on {self.address[0]}:{self.address[1]}")

        known_nodes = SortedSet(key = lambda n: n[0])
        if address == "":
            self.logger.info("No adress specified, finding chords online ...")
            self.add_node(known_nodes, *self.get_chord_nodes())
            if len(known_nodes) == 0:
                self.logger.info("No available chord nodes found. Exiting.")
                return
        else:
            address = parse_address(address)
            address = (address[0], address[1])
            self.add_node(known_nodes, address)
        
        t = Thread(target=self.communicate_with_chord, args=(known_nodes,), daemon=True)
        t.start()
        
        poller = zmq.Poller()
        register_socks(poller, self.usr_send_pipe[0], sys.stdin)
        if self.gui_sock is not None:
            register_socks(poller, self.gui_sock)
        
        self.logger.info("Input ready:")
        recieved = 0
        while self.online:
            socks = dict(poller.poll(1000))
            if self.usr_send_pipe[0] in socks:
                url, html, url_list = self.usr_send_pipe[0].recv_pyobj(zmq.NOBLOCK)
                self.logger.info(f"({recieved})Recieved {url}: Links({len(url_list)})")
                recieved += 1
                if self.gui_sock is not None:
                    self.gui_sock.send_pyobj((url, html, url_list))

            elif sys.stdin.fileno() in socks:
                for line in sys.stdin:
                    client_request = parse_requests(line)
                    if len(client_request) != 0:
                        self.usr_send_pipe[0].send_pyobj(client_request)
                    break
                self.logger.info("Input ready:")
            
            elif self.gui_sock in socks:
                gui_request = self.gui_sock.recv_pyobj(zmq.NOBLOCK)
                client_request = parse_requests(gui_request)
                self.logger.debug(str(client_request))
                self.usr_send_pipe[0].send_pyobj(client_request)

    def communicate_with_chord(self, known_nodes:SortedSet):
        comm_sock = get_router(self.context)
        search_trees = []

        connected = set()
        pending_recv = dict()
        
        poller = zmq.Poller()
        register_socks(poller, comm_sock, self.usr_send_pipe[1])
        
        while self.online:
            socks = dict(poller.poll(TIMEOUT_COMM*MAX_IDDLE*1000))
            if self.usr_send_pipe[1] in socks:
                client_requests:Tuple = self.usr_send_pipe[1].recv_pyobj()
                url_list = [url for url, _ in client_requests]
                for url, depth in client_requests:
                    add_search_tree(search_trees, url, depth)
            
            elif comm_sock in socks:
                _, flag, message = comm_sock.recv_multipart()
                if flag == REP_CLIENT_NODE:
                    (url_request, next_node) = pickle.loads(message)
                    self.add_node(known_nodes, next_node)
                    url_list = [url_request]
                    reset_times(url, known_nodes, pending_recv, time.time() + 0.6,  self.bits) 
                
                if flag == REP_CLIENT_INFO:
                    url, html, url_set = pickle.loads(message)
                    url = remove_back_slashes(url)
                    try:
                        del pending_recv[url]
                    except KeyError:
                        pass
                    self.usr_send_pipe[1].send_pyobj((url, html, url_set)) # Send recieved url and html to main thread for display
                    self.local_cache[url] = (html, url_set)
                    url_set = update_search_trees(search_trees, url, url_set)
                    url_list = [*url_set]
            else:
                self.logger.debug(f"pending: {len([*pending_recv])}, st: {search_trees})")
                url_list = [*pending_recv]

            for url in url_list:
                if url in self.local_cache:
                    (html, url_set) = self.local_cache[url]
                    url_list2 = [*update_search_trees(search_trees, url, url_set)] #if urlx not in pending_recv]
                    print(url_list2, url, len(url_set))
                    url_list += url_list2
                    self.usr_send_pipe[1].send_pyobj((url, html, url_set))
                    continue
                
                url = remove_back_slashes(url)
                if url not in pending_recv:
                    pending_recv[url] = time.time() + 0.5
                idx, target_addr = self.target_node(url, known_nodes)
                if time.time() - 2*TIMEOUT_COMM*MAX_IDDLE > pending_recv[url]:
                    self.logger.debug(f"Removing {idx} due to delayed response")
                    reset_times(url, known_nodes, pending_recv, time.time() + 0.5, self.bits)
                    known_nodes.remove((idx, (target_addr[0], target_addr[1] - 1))) 
                    _, target_addr = self.target_node(url, known_nodes)
                    if target_addr is None:
                        break

                message = pickle.dumps((url, self.address))
                
                if not target_addr in connected:
                    connect_router(comm_sock, target_addr)
                    connected.add(target_addr)
                comm_sock.send_multipart([address_to_string(target_addr).encode(), message])

        
        comm_sock.close()
        self.logger.info("Comunication with chord closing")

    def target_node(self, url, known_nodes) -> Tuple[int, tuple]:
        if len(known_nodes) == 0:
            self.logger.info("Re-connecting to net")
            self.add_node(known_nodes, *self.get_chord_nodes())
            if len(known_nodes) == 0:
                self.logger.info("No online chord nodes found")
                self.online = False
                return None, None
        
        return select_target_node(url, known_nodes, self.bits)

    def add_node(self, known_nodes:SortedSet, *nodes_addr):
        for addr in nodes_addr:
            addr_hash = f"{addr[0]}:{addr[1]}"
            idx = get_id(addr_hash) % (2 ** self.bits)
            known_nodes.add((idx, addr))

    def get_chord_nodes(self):
        chord_nodes = find_nodes(
            port=CHORD_BEACON_PORT,
            code_word=CODE_WORD_CHORD,
            tolerance=3,
            all = True
        )
        self.logger.debug(f"Getting chord nodes: {chord_nodes}")
        return chord_nodes
        