import logging

from requests.api import request
from scraper.scraper_const import MAX_IDDLE, TIMEOUT_COMM
from scrap_chord.util import add_search_tree, add_to_dict, in_between, parse_requests, remove_back_slashes, update_search_trees
import sys
import pickle
from threading import  Thread
from typing import Tuple
from utils.const import CHORD_BEACON_PORT, CODE_WORD_CHORD, REP_CLIENT_INFO, REP_CLIENT_NODE

import zmq.sugar as zmq
from sortedcontainers import SortedSet

from utils.tools import address_to_string, connect_router, find_nodes, get_id, get_router, get_source_ip, parse_address, register_socks, zpipe


class ScrapChordClient:
    def __init__(self, port, m) -> None:
        self.address = (get_source_ip(), port)
        
        self.context = zmq.Context()
        self.usr_send_pipe = zpipe(self.context)

        self.bits = m
        self.local_cache = dict()
        self.online = True
        
        # debbug & info
        logging.basicConfig(format = "%(name)s: %(levelname)s: %(message)s", level=logging.DEBUG)
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
        self.logger.info("Input ready:")
        while self.online:
            socks = dict(poller.poll())
            if self.usr_send_pipe[0] in socks:
                url, html, url_list = self.usr_send_pipe[0].recv_pyobj(zmq.NOBLOCK)
                self.logger.info(f"Recieved {url}\n {html[:100]} \n ... \n URLS({len(url_list)}):\n" + "\n".join(str(urlx) for urlx in url_list)) # Print url and first 100 chars from html
            if sys.stdin.fileno() in socks:
                for line in sys.stdin:
                    client_request = parse_requests(line)
                    if len(client_request) != 0:
                        self.logger.debug(f"Sending to pyobj: {client_request}")
                        self.usr_send_pipe[0].send_pyobj(client_request)
                    break
                self.logger.info("Input ready:")

    def communicate_with_chord(self, known_nodes:SortedSet):
        comm_sock = get_router(self.context)
        search_trees = []

        connected = set()
        pending_recv = dict() #SortedSet()
        poller = zmq.Poller()
        register_socks(
            poller, comm_sock, self.usr_send_pipe[1]
        )
        while self.online:
            socks = dict(poller.poll(TIMEOUT_COMM*MAX_IDDLE*1000))
            if self.usr_send_pipe[1] in socks:
                client_requests:Tuple = self.usr_send_pipe[1].recv_pyobj()
                self.logger.debug(f"Recieving request: {client_requests}")
                url_list = [url for url, _ in client_requests]
                for url, depth in client_requests:
                    add_search_tree(search_trees, url, depth)
            
            elif comm_sock in socks:
                _, flag, message = comm_sock.recv_multipart()
                self.logger.debug(f"Recieving reply to request: {flag}")
                if flag == REP_CLIENT_NODE:
                    next_node = pickle.loads(message)
                    self.add_node(known_nodes, next_node)
                    url_list = [*pending_recv]
                
                if flag == REP_CLIENT_INFO:
                    url, html, url_list = pickle.loads(message)
                    url = remove_back_slashes(url)
                    if url in self.local_cache:
                        continue
                    self.local_cache[url] = (html, url_list)
                    url_list = [*update_search_trees(search_trees, url, url_list)]
                    del pending_recv[url]
                    self.usr_send_pipe[1].send_pyobj((url, html, url_list)) # Send recieved url and html to main thread for display
            else:
                url_list = [*pending_recv]

            for url in url_list:
                if url in self.local_cache:
                    (html, urlx_list) = self.local_cache[url]
                    url_list += [urlx for urlx in update_search_trees(search_trees, url, urlx_list) if urlx not in pending_recv]
                    self.usr_send_pipe[1].send_pyobj((url, html, urlx_list))
                    continue
                
                url = remove_back_slashes(url)
                add_to_dict(pending_recv, url)
                idx, target_addr = self.select_target_node(url, known_nodes)
                if  pending_recv[url] > 4:
                    self.logger.debug(f"Removing {idx} because of its delay with {url}")
                    known_nodes.remove((idx, (target_addr[0], target_addr[1] - 1))) 
                    _, target_addr = self.select_target_node(url, known_nodes)
                    if target_addr is None:
                        break
                    pending_recv[url] = 1

                message = pickle.dumps((url, self.address))
                self.logger.debug(f"Sending {url} to {target_addr}")
                
                if not target_addr in connected:
                    connect_router(comm_sock, target_addr)
                    connected.add(target_addr)
                comm_sock.send_multipart([address_to_string(target_addr).encode(), message])
        
        comm_sock.close()
        self.logger.info("Comunication with chord closing")

    def select_target_node(self, url, known_nodes) -> Tuple[int, tuple]:
        if len(known_nodes) == 0:
            self.logger.info("Re-connecting to net")
            self.add_node(known_nodes, *self.get_chord_nodes())
            if len(known_nodes) == 0:
                self.logger.info("No online chord nodes found")
                self.online = False
                return None, None
        
        if len(known_nodes) == 1:
            return (known_nodes[0][0], (known_nodes[0][1][0], known_nodes[0][1][1] + 1))
        url_id = get_id(url) % (2 ** self.bits)
        self.logger.debug(f"Selecting target of {url_id} from {known_nodes}")
        lwb_id, _ = known_nodes[-1]
        for node_id, addr in known_nodes:
            if in_between(self.bits, url_id, lwb_id, node_id):
                return (node_id, (addr[0], addr[1] + 1))
            lwb_id = node_id
        
        raise Exception("A node must be always found")

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