from itertools import combinations_with_replacement
import logging
import pickle
import re
import sys
import random
from threading import Thread
from typing import Tuple
from sortedcontainers.sortedset import SortedSet

from zmq.sugar import poll
from utils.tools import connect_router, find_nodes, get_id, get_router, net_beacon, recieve_multipart_timeout, recv_from_router, register_socks, zpipe
import zmq.sugar as zmq
import socket
from utils.const import CHORD_BEACON_PORT, CODE_WORD_CHORD, CODE_WORD_SCRAP, REP_CLIENT_INFO, REP_CLIENT_NODE, REP_SCRAP_ACK_CONN, REP_SCRAP_ASOC_YES, REQ_SCRAP_ACK, REQ_SCRAP_ASOC, SCRAP_BEACON_PORT
from pychord import ChordNode


class ScrapChordNode(ChordNode):
    def __init__(self, m, port, visible=True) -> None:
        super().__init__(m, port)
        self.online = False
        self.visible = visible
        self.cache = dict()
        self.scraper_list = []

        self.chord_push_pipe = zpipe(self.context)
        self.chord_scrap_pipe = zpipe(self.context)
        
        logging.basicConfig(format = "scrapkord: %(levelname)s: %(message)s", level=logging.INFO)
        self.logger = logging.getLogger("scrapkord")

    def run(self):
        self.start_chord_functionality()

        # comm_client = Thread(target=self.communicate_with_client)
        comm_scrap = Thread(target=self.communicate_with_scraper)
        push_pull = Thread(target=self.push_pull_work)
        
        # comm_client.start()
        comm_scrap.start()
        push_pull.start()

        if self.visible:
            Thread(
                target=net_beacon,
                args=(self.address[1] + 1, CHORD_BEACON_PORT, CODE_WORD_CHORD),
                daemon=True
            )
        self.communicate_with_client()

    def communicate_with_client(self):
        comm_sock = get_router(self.context)
        comm_sock.bind(f"tcp://{self.address[0]}:{self.address[1] + 1}")

        router_table = dict()
        request_table = dict()
        # addr_byte = pickle.dumps(self.address)

        poller = zmq.Poller()
        register_socks(poller, self.chord_push_pipe[0], comm_sock)
        while self.online:
            socks = dict(poller.poll(2000))
            if comm_sock in socks:
                req = comm_sock.recv_multipart(zmq.NOBLOCK)

                idx, message = req
                url_request, client_addr = pickle.loads(message)
                url_node_addr = self.url_succesor(url_request)

                if self.address == url_node_addr:
                    self.register_request(url_request, client_addr, request_table)
                    router_table[client_addr] = idx
                    self.chord_push_pipe[0].send_pyobj(url_request)
                    self.chord_scrap_pipe[0].send_multipart([REQ_SCRAP_ASOC])
                else:
                    node_addr_byte = pickle.dumps(url_node_addr)
                    comm_sock.send_multipart([idx, REP_CLIENT_NODE, node_addr_byte])
            
            elif self.chord_push_pipe[0] in socks:
                url, html, url_list = self.chord_push_pipe[0].recv_pyobj()
                for addr in request_table[url]:
                    idx = router_table[addr]
                    message = pickle.dumps((url, html, url_list))
                    comm_sock.send_multipart([idx, REP_CLIENT_INFO, message])
                del request_table[url]
            
            else:
                for url in request_table:
                    self.chord_push_pipe[0].send_pyobj(url_request)
                    self.chord_scrap_pipe[0].send_multipart([REQ_SCRAP_ASOC])
    
    def url_succesor(self, url:str) -> Tuple[str, int]:
        url_id = get_id(url)
        n = self.find_successor(url_id)
        return n[1]
        
                
    def communicate_with_scraper(self):
        comm_sock = get_router(self.context)
        pending_messages = []
        connected_to = []

        while self.online:
            if len(pending_messages) == 0:
                req = recieve_multipart_timeout(self.chord_scrap_pipe[1], 0.5)
            else:
                req = pending_messages.pop(-1)
            if len(req) > 0:
                flag = req[0]
                while len(self.scraper_list) > 0:
                    if len(connected_to) == 0:
                        x = random.randint(0, len(self.scraper_list) - 1)
                    else:
                        x = connected_to.pop(-1)
                    
                    addr = self.scraper_list[x]
                    info = pickle.dumps(self.address)
                    connect_router(comm_sock, addr)
                    comm_sock.send_multipart([addr.encode(), flag,  info])
                    rep, _ = recv_from_router(comm_sock, addr)
                    if len(rep) == 0:
                        self.scraper_list.pop(x)
                        continue
                    if rep == REP_SCRAP_ASOC_YES:
                        connected_to.append(x)
                        break
                else:
                    pending_messages.append(req)
                    self.scraper_list = self.get_online_scrappers()
                    if len(self.scraper_list) == 0:
                        self.logger.error("No scrapper found, retrying ...")
                        
    
    def get_online_scrappers(self):
        scrap_addrs = find_nodes(
            port=SCRAP_BEACON_PORT,
            code_word=CODE_WORD_SCRAP,
            tolerance=3,
            all=True
        )
        if len(scrap_addrs) == 0:
            scrap_addrs = self.find_scrappers_chord()

        return scrap_addrs

    def push_pull_work(self):
        push_sock = self.context.socket(zmq.PUSH)
        pull_sock = self.context.socket(zmq.PULL)
        push_sock.bind(f"tcp://{self.address[0]}:{self.address[1] + 2}")
        push_sock.bind(f"tcp://{self.address[0]}:{self.address[1] + 3}")
        
        poller = zmq.Poller()
        register_socks(poller, pull_sock, self.chord_push_pipe[1])
        while self.online:
            socks = poller.poll(1500)
            if self.chord_push_pipe[1] in socks:
                request_url = self.chord_push_pipe[1].recv_pyobj(zmq.NOBLOCK)
                push_sock.send_pyobj(request_url)
            if pull_sock in socks:
                obj = pull_sock.recv_pyobj()
                self.chord_push_pipe[1].send_pyobj(obj)
        
    def register_request(request, request_giver, request_table):
        try:
            request_table[request].add(request_giver)
        except KeyError:
            request_table[request] = set([request_giver])

    def get_scrappers(self):
        return self.scraper_list

    def find_scrappers_chord(self):
        scrappers_found = []
        current_node = (self.node_id, self.address)
        successor = self.successor()

        while (
            not scrappers_found and successor is not None and successor != current_node
        ):
            scrappers = self.rpc(successor, "get_scrappers")
            if scrappers is not None:
                scrappers_found = [scrapper for scrapper in scrappers]

            successor = self.rpc(successor, "successor")

        return scrappers_found
