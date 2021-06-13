import logging
from utils.tools import recieve_multipart_timeout
import zmq.sugar as zmq
import socket
from ..utils.const import BEACON_PORT, SCRAP_CHORD_BEACON_PORT
from ..pychord import ChordNode

class ScrapChordNode(ChordNode):
    def __init__(self, idx, m, ip, port) -> None:
        super().__init__(idx, m, ip, port)
        self.online = False
        self.scrapper_list = []
        
        logging.basicConfig(format = "scrapper: %(levelname)s: %(message)s", level=logging.INFO)
        self.logger = logging.getLogger("scrapper")

    def communicate_with_scrapper(self):
        comm_sock = self.context.socket(zmq.ROUTER)
        comm_sock.probe_router = 1
        
        id_ip_tabe = dict()
        

        while self.online:
            if len(self.scrapper_list) == 0:
                scrapper_addrs = self.find_scrapper(tolerance=3)
                if scrapper_addrs == "":
                    self.logger.warning("No online scrapper found")
                    continue
    
    def connect_to_scrapper(self, sock, address, id_ip_stable):
        sock.connect(f"tcp://{address}")
        req = recieve_multipart_timeout(sock, 3)
        if req == 0:
            self.logger.info(f"Could not connect to scrapper at")
        sock.send_multipart()

    def find_scrapper(self, tolerance) -> str:
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
                    print(f"Found on-line peer: {addr[0]}:{BEACON_PORT - 1}")
                    broadcast_socket.close()
                    return addr[0]
            except socket.timeout:
                tolerance -= 1
        broadcast_socket.close()
        return ""