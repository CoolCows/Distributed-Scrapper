import logging
import pickle
import re
import sys
import random
from sortedcontainers.sortedset import SortedSet

from zmq.sugar import poll
from utils.tools import connect_router, get_router, recieve_multipart_timeout, recv_from_router, register_socks, zpipe
import zmq.sugar as zmq
import socket
from ..utils.const import BEACON_PORT, REP_SCRAP_ACK_CONN, REP_SCRAP_ASOC_YES, REQ_SCRAP_ACK, REQ_SCRAP_ASOC, SCRAP_CHORD_BEACON_PORT
from ..pychord import ChordNode

class ScrapChordNode(ChordNode):
    def __init__(self, idx, m, ip, port) -> None:
        super().__init__(idx, m, ip, port)
        self.online = False
        self.scraper_list = SortedSet()
        self.cache = dict()

        self.chord_push_pipe = zpipe(self.context)
        self.chord_scrap_pipe = zpipe(self.context)
        
        logging.basicConfig(format = "scrapper: %(levelname)s: %(message)s", level=logging.INFO)
        self.logger = logging.getLogger("scrapper")
    
    def communicate_with_client(self):
        comm_sock = get_router(self.context)
        comm_sock.bind(f"tcp://{self.address[0]}:{self.address[1]}")

        router_table = dict()
        request_table = dict()

        poller = zmq.Poller()
        register_socks(poller, self.chord_push_pipe[0], comm_sock)
        while self.online:
            socks = dict(poller.poll(1500))
            if comm_sock in socks:
                req = comm_sock.recv_multipart(zmq.NOBLOCK)
                if len(req) == 2 and req[2] == b"":
                    comm_sock.send_multipart([req[0]])
                    continue

                idx, message = req
                url_request, client_addr = pickle.loads(message)
                self.register_request(url_request, client_addr, request_table)
                router_table[client_addr] = idx
                self.chord_push_pipe[0].send_pyobj(url_request)
                self.chord_scrap_pipe[0].send_multipart([REQ_SCRAP_ASOC])
            
            if self.chord_push_pipe[0] in socks:
                url, html, url_list = self.chord_push_pipe[0].recv_pyobj()
                for addr in request_table[url]:
                    idx = router_table[addr]
                    message = pickle.dumps((url, html, url_list))
                    comm_sock.send_multipart([idx, message])
                request_table[url] = set()
                
    def communicate_with_scraper(self):
        comm_sock = get_router(self.context)
        pending_messages = []

        while self.online:
            if len(pending_messages) == 0:
                req = recieve_multipart_timeout(self.chord_scrap_pipe[1], 0.5)
            else:
                req = pending_messages.pop(-1)
            if len(req) > 0:
                flag = req[0]
                while len(self.scraper_list) > 0:
                    x = random.randint(0, len(self.scraper_list) - 1)
                    addr = self.scraper_list[x]
                    info = pickle.dumps(self.address)
                    comm_sock.send_multipart([addr.encode(), flag,  info])
                    rep, _ = recv_from_router(comm_sock, addr)
                    if len(rep) == 0:
                        self.scraper_list.pop(x)
                        continue
                    if rep == REP_SCRAP_ASOC_YES:
                        break
                else:
                    self.find_scraper(3)
                    pending_messages.append(req)
            else:
                pass
    

    def connect_to_scraper(self, sock, address:str, router_table):
        connect_router(sock, address)
        b_addr = address.encode()
    
    def connect_to_other_scrapper(self):
        if len(self.scraper_list) == 0:
            scrapper_addrs = self.find_scraper(tolerance=3)
            if scrapper_addrs == "":
                self.logger.warning("No online scrapper found")
            
            # self.connect_to_scraper(comm_sock, scrapper_addrs, router_table)


     
    def push_pull_work(self):
        push_sock = self.context.socket(zmq.PUSH)
        pull_sock = self.context.socket(zmq.PULL)
        push_sock.bind("tcp://")
        
        poller = zmq.Poller()
        poller.register(pull_sock, zmq.POLLIN)
        poller.register(self.chord_push_pipe[1], zmq.POLLIN)
        while self.online:
            socks = poller.poll(1500)
            if self.chord_push_pipe[1] in socks:
                request_url = self.chord_push_pipe[1].recv_pyobj(zmq.NOBLOCK)
                push_sock.send_pyobj(request_url)
            if pull_sock in socks:
                obj = pull_sock.recv_pyobj()
                self.chord_push_pipe[1].send_pyobj(obj)
    
    def find_scraper(self, tolerance) -> str:
        self.logger.debug("Searching for online scrappers")
        broadcast_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_socket.settimeout(0.5)
        while tolerance > 0:
            broadcast_socket.sendto(
                b"PING",
                ("<broadcast>", BEACON_PORT)
            )
            try:
                info, addr = broadcast_socket.recvfrom(1024)
                if info == b"PONG":
                    self.logger.debug(f"Found on-line scrapper at {addr[0]}:{BEACON_PORT - 1}")
                    broadcast_socket.close()
                    return addr[0]
            except socket.timeout:
                tolerance -= 1
        broadcast_socket.close()
        return ""
    
    def register_scraper(self, scraper_addr, router_table, idx):
        router_table[scraper_addr] = idx
        self.scraper_list.add(scraper_addr)
    
    def unregister_scrapper(self, scraper_addr, router_table):
        self.scraper_list.remove(scraper_addr)
        del router_table[scraper_addr]

    def register_request(request, request_giver, request_table):
        try:
            request_table[request].add(request_giver)
        except KeyError:
            request_table[request] = set([request_giver])
