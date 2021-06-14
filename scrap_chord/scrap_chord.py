import logging
import sys
from utils.tools import recieve_multipart_timeout, zpipe
import zmq.sugar as zmq
import socket
from ..utils.const import BEACON_PORT, SCRAP_CHORD_BEACON_PORT
from ..pychord import ChordNode


class ScrapChordNode(ChordNode):
    def __init__(self, idx, m, ip, port) -> None:
        super().__init__(idx, m, ip, port)
        self.online = False
        self.scrapper_list = set()

        self.usr_push_pipe = zpipe(self.context)
        self.usr_pull_pipe = zpipe(self.context)

        logging.basicConfig(
            format="scrapper: %(levelname)s: %(message)s", level=logging.INFO
        )
        self.logger = logging.getLogger("scrapper")

    def communicate_with_client(self):
        comm_sock = self.context.socket(zmq.ROUTER)
        comm_sock.bind(f"tcp://{self.address[0]}:{self.address[1]}")

    def communicate_with_scrapper(self):
        comm_sock = self.context.socket(zmq.ROUTER)
        comm_sock.probe_router = 1

        ip_id_table = dict()
        while self.online:
            if len(self.scrapper_list) == 0:
                scrapper_addrs = self.find_scrapper(tolerance=3)
                if scrapper_addrs == "":
                    self.logger.warning("No online scrapper found")
                    continue
                self.connect_to_scrapper(comm_sock, scrapper_addrs, ip_id_table)

    # TODO: Bind push and pull sock to certain location
    def push_work(self):
        push_sock = self.context.socket(zmq.PUSH)
        while True:
            req = self.usr_push_pipe[1].recv_multipart()
            # TODO: Send multipart

    def pull_work(self):
        pull_sock = self.context.socket(zmq.PULL)
        while True:
            obj = pull_sock.recv_pyobj()
            self.usr_pull_pipe[1].send_pyobj(obj)

    def connect_to_scrapper(self, sock, address, ip_id_table):
        sock.connect(f"tcp://{address}")
        req = recieve_multipart_timeout(sock, 3)
        if req == 0:
            self.logger.info(f"Could not connect to scrapper at {address}")
            return
        ip_id_table[address] = req[0]
        self.scrapper_list.add(address)

    def find_scrapper(self, tolerance) -> str:
        self.logger.debug("Searching for online scrappers")
        broadcast_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_socket.settimeout(0.5)
        while tolerance > 0:
            broadcast_socket.sendto(b"PING", ("<broadcast>", BEACON_PORT))
            try:
                info, addr = broadcast_socket.recvfrom(1024)
                if info == b"PONG":
                    self.logger.debug(
                        f"Found on-line scrapper at {addr[0]}:{BEACON_PORT - 1}"
                    )
                    broadcast_socket.close()
                    return addr[0]
            except socket.timeout:
                tolerance -= 1
        broadcast_socket.close()
        return ""

    def get_scrappers(self):
        return self.scrapper_list

    def find_scrapppers_chord(self):
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
