import logging
import time
from utils.search_tree import SearchTree
from requests.api import request
from scraper.scraper_const import MAX_IDDLE, TIMEOUT_COMM
from scrap_chord.util import add_search_tree, in_between, parse_requests, remove_back_slashes, reset_times, select_target_node, update_search_trees
import sys
import pickle
from threading import  Thread
from typing import List, Tuple
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
                obj = self.usr_send_pipe[0].recv_pyobj(zmq.NOBLOCK)
                if len(obj) == 3:
                    url, html, url_list = obj
                    self.logger.info(f"({recieved})Recieved url: Links({len(url_list)})")
                    recieved += 1
                    if self.gui_sock is not None:
                        self.gui_sock.send_pyobj(obj)
                elif len(obj) == 2:
                    self.logger.info(obj[0])
                    if self.gui_sock is not None:
                        self.gui_sock.send_pyobj(obj)

            if sys.stdin.fileno() in socks:
                for line in sys.stdin:
                    if line.split()[0] == "ns":
                        print(known_nodes)
                        break
                    if line.split()[0] == "of":
                        self.online = False
                        break
                    client_request = parse_requests(line)
                    if len(client_request) != 0:
                        self.usr_send_pipe[0].send_pyobj(client_request)
                    break
                self.logger.info("Input ready:")
            
            if self.gui_sock is not None and self.gui_sock in socks:
                gui_request = self.gui_sock.recv_pyobj(zmq.NOBLOCK)
                client_request = parse_requests(gui_request)
                self.logger.debug(str(client_request))
                self.usr_send_pipe[0].send_pyobj(client_request)

    def communicate_with_chord(self, known_nodes:SortedSet):
        comm_sock = get_router(self.context)
        comm_sock.linger = 0
        # comm_sock.snd_timeo = 1000
        search_trees = []

        connected = set()
        pending_recv = dict()
        connection_lost = 1

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
                _, flag, message = comm_sock.recv_multipart(zmq.NOBLOCK)
                if flag == REP_CLIENT_NODE:
                    (url_request, next_node) = pickle.loads(message)
                    connection_lost += 1
                    self.add_node(known_nodes, next_node)
                    url_list = [url_request]
                    reset_times(url_request, known_nodes, pending_recv, connection_lost*(time.time() + 0.6),  self.bits) 
                
                if flag == REP_CLIENT_INFO:
                    url, html, url_set = pickle.loads(message)
                    url = remove_back_slashes(url)
                    try:
                        del pending_recv[url]
                    except KeyError:
                        pass
                    self.usr_send_pipe[1].send_pyobj((url, html, url_set)) # Send recieved url and html to main thread for display
                    self.local_cache[url] = (html, url_set)
                    
                    url_set, completed = update_search_trees(search_trees, url, url_set)
                    self.send_all_search_trees(completed)
                    url_list = [*url_set]
            else:
                self.logger.debug(f"pending: {len([*pending_recv])}, cl: {connection_lost} st: {search_trees})")
                url_list = [*pending_recv]
            
            temp_len = len(known_nodes)
            temp_cl = connection_lost
            for url in url_list:
                if url in self.local_cache:
                    (html, url_set) = self.local_cache[url]
                    more_urls, completed = update_search_trees(search_trees, url, url_set) #
                    more_urls = [urlx for urlx in more_urls if urlx not in pending_recv] 
                    url_list += more_urls
                    self.usr_send_pipe[1].send_pyobj((url, html, url_set))
                    self.send_all_search_trees(completed)
                    continue
                
                url = remove_back_slashes(url)
                if url not in pending_recv:
                    pending_recv[url] = time.time() + 0.5
                idx, target_addr = self.target_node(url, known_nodes)

                # self.logger.debug(f"Time remainging = {time.time() - 2*TIMEOUT_COMM*MAX_IDDLE - pending_recv[url]} for {url}")

                if time.time() - 2*TIMEOUT_COMM*MAX_IDDLE > pending_recv[url]:
                    self.logger.debug(f"Removing {idx} due to delayed response({connection_lost})")
                    reset_times(url, known_nodes, pending_recv, time.time() + 0.5, self.bits)
                    known_nodes.remove((idx, (target_addr[0], target_addr[1] - 1))) 
                    idx, new_target_addr = self.target_node(url, known_nodes)
                    if new_target_addr is None:
                        self.online = False
                        break
                    if new_target_addr == target_addr:
                        connection_lost += 1
                        # self.logger.debug(f"Increasing connection: {connection_lost}")
                        reset_times(url, known_nodes, pending_recv, connection_lost*5 + (time.time() + 0.5), self.bits)

                message = pickle.dumps((url, self.address))
                
                if not target_addr in connected:
                    connect_router(comm_sock, target_addr)
                    connected.add(target_addr)
                comm_sock.send_multipart([address_to_string(target_addr).encode(), message])
            
            if temp_len == len(known_nodes) and temp_cl == connection_lost:
                connection_lost = max(1, connection_lost - 1)
        
        comm_sock.close()
        self.logger.info("Comunication with chord closing")

    def target_node(self, url, known_nodes) -> Tuple[int, tuple]:
        if len(known_nodes) == 0:
            self.logger.info("Re-connecting to net")
            self.add_node(known_nodes, *self.get_chord_nodes())
            if len(known_nodes) == 0:
                self.logger.info("No online chord nodes found")
                return None, None

        return select_target_node(url, known_nodes, self.bits)

    def add_node(self, known_nodes:SortedSet, *nodes_addr):
        for addr in nodes_addr:
            addr_hash = f"{addr[0]}:{addr[1]}"
            idx = get_id(addr_hash, self.bits)
            known_nodes.add((idx, addr))

    def get_chord_nodes(self):
        chord_nodes, _ = find_nodes(
            port=CHORD_BEACON_PORT,
            code_word=CODE_WORD_CHORD,
            tolerance=3,
            all = True
        )
        self.logger.debug(f"Getting chord nodes: {chord_nodes}")
        return chord_nodes
    
    def send_all_search_trees(self, completed:List[SearchTree]):
        for st in completed:
            self.usr_send_pipe[1].send_pyobj(st.visual(self.local_cache, basic=False))
        