import logging
import pickle
import random
from scraper.scraper_const import MAX_IDDLE, TIMEOUT_WORK
from threading import Lock, Thread
import time
from typing import Tuple
from sortedcontainers.sortedset import SortedSet

from utils.tools import address_to_string, connect_router, find_nodes, get_id, get_router, net_beacon, parse_address, recieve_multipart_timeout, recv_from_router, register_socks, zpipe
import zmq.sugar as zmq
from utils.const import CHORD_BEACON_PORT, CODE_WORD_CHORD, CODE_WORD_SCRAP, REP_CLIENT_INFO, REP_CLIENT_NODE, REP_SCRAP_ACK_CONN, REP_SCRAP_ASOC_YES, REQ_SCRAP_ACK, REQ_SCRAP_ASOC, SCRAP_BEACON_PORT
from pychord import ChordNode


class ScrapChordNode(ChordNode):
    def __init__(self, port, m, visible=True) -> None:
        super().__init__(m, port)
        self.online = False
        self.visible = visible
        self.cache = dict()
        self.scraper_list = []
        
        self.last_pull = 0
        self.last_pull_lock = Lock()

        self.push_scrap_pipe = zpipe(self.context)
        self.chord_scrap_pipe = zpipe(self.context)
        
        logging.basicConfig(format = "%(name)s: %(levelname)s: %(message)s", level=logging.DEBUG)
        self.logger = logging.getLogger("scrapkord")

    def run(self, addr:str=""):
        self.online = True
        self.logger.info(f"ScrapKord({self.node_id}) running on {self.address[0]}:{self.address[1]}")
        
        if addr != "":
            self.join(get_id(addr), parse_address(addr))
        else:
            net_nodes = find_nodes(
                port=CHORD_BEACON_PORT,
                code_word=CODE_WORD_CHORD,
                tolerance=3,
                all=True
            )
            if len(net_nodes) > 0:
                for node_addr in net_nodes:
                    self.add_node((get_id(address_to_string((node_addr[0], node_addr[1] - 1))) % (2**self.bits), (node_addr[0], node_addr[1] - 1)))
                    succ_node = self.pop_node(0)
                    self.join(succ_node[0], succ_node[1])
                    self.logger.debug(f"Joined to {succ_node}")
            else:
                self.logger.debug(f"Alone in the net")
                self.join()

        base_routine = Thread(target=self.start_chord_base_routine)
        # comm_client = Thread(target=self.communicate_with_client)
        comm_scrap = Thread(target=self.communicate_with_scraper)
        push_pull = Thread(target=self.push_pull_work)
        
        base_routine.start()
        # comm_client.start()
        comm_scrap.start()
        push_pull.start()

        if self.visible:
            self.logger.debug("Network discovery is ON")
            Thread(
                target=net_beacon,
                args=(self.address[1] + 1, CHORD_BEACON_PORT, CODE_WORD_CHORD),
                daemon=True
            ).start()
    
        self.communicate_with_client()
        #self.communicate_with_scraper()

    def communicate_with_client(self):
        self.logger.debug(f"CliCom: Router binded to {self.address[0]}:{self.address[1] + 1}")
        comm_sock = get_router(self.context)
        comm_sock.bind(f"tcp://{self.address[0]}:{self.address[1] + 1}")

        router_table = dict()
        request_table = dict()

        poller = zmq.Poller()
        register_socks(poller, self.chord_scrap_pipe[0], comm_sock)
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
                    self.chord_scrap_pipe[0].send_pyobj(url_request)
                else:
                    node_addr_byte = pickle.dumps(url_node_addr)
                    comm_sock.send_multipart([idx, REP_CLIENT_NODE, node_addr_byte])
            
            elif self.chord_scrap_pipe[0] in socks:
                self.logger.debug("CliCom: Recieved work forwarding it to client")
                url, html, url_list = self.chord_scrap_pipe[0].recv_pyobj()
                for addr in request_table[url]:
                    idx = router_table[addr]
                    message = pickle.dumps((url, html, url_list))
                    comm_sock.send_multipart([idx, REP_CLIENT_INFO, message])
                request_table[url] = set()

        self.logger.debug("CliCom: Closing")
    
    def url_succesor(self, url:str) -> Tuple[str, int]:
        url_id = get_id(url)
        return self.address
        n = self.find_successor(url_id)
        return n[1]
                
    def communicate_with_scraper(self):
        self.logger.debug("CommScrap: Running")
        pending_messages = SortedSet()
        last_connected = []
        
        poll = zmq.Poller()
        register_socks(poll, self.chord_scrap_pipe[1], self.push_scrap_pipe[1])
        while self.online:
            socks = dict(poll.poll(500))
            connected_to_any_scraper = time.time() < self.last_pull

            if self.chord_scrap_pipe[1] in socks:
                # recv url
                url = self.chord_scrap_pipe[1].recv_pyobj(zmq.NOBLOCK)
                if url in pending_messages:
                    continue
                if url in self.cache:
                    html, url_list =  self.cache[url]
                    self.chord_scrap_pipe[1].send_pyobj((url, html, url_list))
                    continue
                self.logger.debug(f"ScrapCom: Request to scrap url {url}")
                pending_messages.add(url)
                # connec to scraper
                if not connected_to_any_scraper:
                    if not self.connect_to_scraper(last_connected):
                        self.logger.debug(f"ScrapCom: cant connect to any scraper")
                        continue
                self.logger.debug(f"ScrapCom: Sent pyobj for work")
                self.push_scrap_pipe[1].send_pyobj(url)

            elif self.push_scrap_pipe[1] in socks:
                # rcv object
                url, html, url_list = self.push_scrap_pipe[1].recv_pyobj(zmq.NOBLOCK)
                if url in self.cache:
                    continue
                self.logger.debug(f"ScrapCom: work done with {url}")
                # remove from pending
                pending_messages.remove(url)
                # store object
                self.cache[url] = (html, url_list) # TODO: Where to save it so data can be replicated (to successor)
                # forward to comm client
                self.chord_scrap_pipe[1].send_pyobj((url, html, url_list))
                
            elif not connected_to_any_scraper and len(pending_messages) > 0:
                if self.connect_to_scraper(last_connected):
                    self.logger.debug(f"ScrapCom: Resending old mesages")
                    for url in pending_messages:
                        self.push_scrap_pipe[1].send_pyobj(url) 
        
        self.logger.debug("ScrapComm: Closing ...")
    
    def push_pull_work(self):
        self.logger.debug(f"PushPull: Running on {self.address[0]}:{self.address[1] + 2}, {self.address[1] + 3}")

        push_sock = self.context.socket(zmq.PUSH)
        pull_sock = self.context.socket(zmq.PULL)
        push_sock.bind(f"tcp://{self.address[0]}:{self.address[1] + 2}")
        pull_sock.bind(f"tcp://{self.address[0]}:{self.address[1] + 3}")
        
        poller = zmq.Poller()
        register_socks(poller, pull_sock, self.push_scrap_pipe[0])
        while self.online:
            socks = dict(poller.poll(1500))
            if self.push_scrap_pipe[0] in socks:
                self.logger.debug("PushPull: Recieved workrding, forwarding")
                request_url = self.push_scrap_pipe[0].recv_pyobj(zmq.NOBLOCK)
                push_sock.send_pyobj(request_url)
            if pull_sock in socks:
                self.logger.debug("PushPull: Recieved work completed, forwarding")
                obj = pull_sock.recv_pyobj()
                self.push_scrap_pipe[0].send_pyobj(obj)
                self.update_last_pull(time.time() + TIMEOUT_WORK*MAX_IDDLE)

        self.logger.debug("PushPull: Closing ...")
    
    def connect_to_scraper(self,  last_connected:list) -> bool:
        comm_sock = get_router(self.context)
        comm_sock.rcvtimeo = 1500
        
        if len(self.scraper_list) == 0:
            self.scraper_list = self.get_online_scrappers()

        while len(self.scraper_list) > 0:
            if len(last_connected) > 0:
                x = last_connected.pop(-1)
            else:
                x = random.randint(0, len(self.scraper_list) - 1)
            
            addr = self.scraper_list[x]
            info = pickle.dumps(self.address)
            
            connect_router(comm_sock, addr)
            comm_sock.send_multipart([address_to_string(addr).encode(), REQ_SCRAP_ASOC,  info])
            try:
                rep = comm_sock.recv_multipart()
            except zmq.error.Again:
                self.scraper_list.pop(x)
                continue
            if rep[1] == REP_SCRAP_ASOC_YES:
                last_connected.append(x)
                self.update_last_pull(time.time() + TIMEOUT_WORK*MAX_IDDLE)
                return True
        return False
    
    def get_online_scrappers(self):
        scrap_addrs = self.find_scrappers_chord()

        if len(scrap_addrs) == 0:
            scrap_addrs = find_nodes(
                port=SCRAP_BEACON_PORT,
                code_word=CODE_WORD_SCRAP,
                tolerance=3,
                all=True
            )   
        return scrap_addrs

    def register_request(self, request, request_giver, request_table):
        try:
            request_table[request].add(request_giver)
        except KeyError:
            request_table[request] = set([request_giver])

    def update_last_pull(self, value):
        self.last_pull_lock.acquire()
        self.last_pull = value
        self.last_pull_lock.release()

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