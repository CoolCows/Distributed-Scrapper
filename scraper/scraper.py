import logging
import requests
import socket
import zmq.sugar as zmq
from bs4 import BeautifulSoup
from threading import Lock, Thread
from .scraper_const import *
from ..utils.tools import parse_address, zpipe, recieve_multipart_timeout, get_source_ip
from ..utils.const import BEACON_PORT, REP_SCRAP_ACK_CONN, REP_SCRAP_ACK_NO_CONN, REP_SCRAP_URL, REQ_SCRAP_ACK, REQ_SCRAP_URL, SCRAP_PORT, REQ_SCRAP_ASOC, REP_SCRAP_ASOC_YES, REP_SCRAP_ASOC_NO


class Scrapper:
    def __init__(self, max_threads:int, visible:bool = False) -> None:
        self.online = False
        self.visible = visible

        self.max_threads = max_threads
        self.worker_threads = [None for _ in range(max_threads)]
        self.num_threads = 0
        
        self.ctx = zmq.Context()
        self.ip = get_source_ip()
        
        # inproc
        self.pipe = zpipe(self.ctx)

        #sync
        self.lock = Lock()

        # debbug & info
        logging.basicConfig(format = "scrapper: %(levelname)s: %(message)s", level=logging.INFO)
        self.logger = logging.getLogger("scrapper")

    def run(self):
        self.logger.info(f"Starting scrapper at {self.ip}:{SCRAP_PORT}")
        self.online = True

        if self.visible:
            Thread(target=self.__scrapper_beacon, daemon=True).start()

        while self.online:
            # In case __communication_loop stops unexpectedly, it starts again
            try:
                self.__communication_loop()
            except Exception as err:
                if isinstance(Exception, KeyboardInterrupt):
                    self.online = False
                else:
                    self.logger.error(f"Error in communication loop, restarting: {err.text}")
        self.logger.info("Waiting for worker threads to finish ...")

    def __communication_loop(self):
        comm_sock = self.ctx.socket(zmq.ROUTER)
        comm_sock.bind(f"tcp://{self.ip}:{SCRAP_PORT}")

        while self.online:
            request = recieve_multipart_timeout(comm_sock, TIMEOUT_COMM)
            if len(request) == 0:
                continue
            if len(request == 2) and request[1] ==  b"":
                comm_sock.send_multipart([request[0], b""])
                continue
            
            sock_id, flag, info = request
            if flag == REQ_SCRAP_ASOC:
                sock_addr = info.decode()
                self.__update_workers()
                if self.num_threads < self.max_threads:
                    self.__create_worker(sock_addr)
                    comm_sock.send_multipart([sock_id, REP_SCRAP_ASOC_YES])
                else:
                    comm_sock.send_multipart([sock_id, REP_SCRAP_ASOC_NO])
            elif flag == REQ_SCRAP_ACK:
                if len([val[0] for val in self.worker_threads if val[0] == sock_addr]) > 0:
                    comm_sock.send_multipart([sock_id, REP_SCRAP_ACK_CONN])
                else:
                    comm_sock.send_multipart([sock_id, REP_SCRAP_ACK_NO_CONN])
            else:
                raise Exception(f"Scrapper: Unknown Flag recieved: {flag}")
        
        comm_sock.close()
             
    def __create_worker(self, addr):
        index = self.worker_threads.index(None)
        t = Thread(target=self.__work_loop, args=(addr, index))
        self.worker_threads[index] = (addr, t)
        self.num_threads += 1
        t.start()

    def __work_loop(self, addr, thread_id):
        ip, port = parse_address(addr)
        pull_sock = self.ctx.socket(zmq.PULL)
        push_sock = self.ctx.socket(zmq.PUSH)
        pull_sock.connect(f"tcp://{ip}:{port}")
        push_sock.connect(f"tcp://{ip}:{port + 1}")

        iddle = 0
        while iddle < MAX_IDDLE: 
            work = recieve_multipart_timeout(pull_sock, TIMEOUT_WORK)
            if len(work) == 0:
                iddle += 1
                continue
            iddle = 0
            info = work[0]
            url = info.decode()

            html, urls = self.__extract_html(url)
            push_sock.send_pyobj((url, html, urls))
        
        self.logger.debug(f"Worker thread({thread_id}) closing.")
        pull_sock.close()
        push_sock.close()

    def __extract_html(self, url):
        self.logger.debug(f"Extracting html from {url}")
        reqs = requests.get(url)
        soup = BeautifulSoup(reqs.text, "html.parser")

        urls = set()
        for link in soup.find_all("a"):
            l = link.get("href")
            urls.add(l)

        return reqs.text, urls

    # def __valid_url(self, url):
    #     parse = urlparse

    def __update_workers(self):
        self.worker_threads = [None for thread in self.worker_threads if thread is None or not thread[1].is_alive()]
        self.num_threads = len([thread for thread in self.worker_threads if thread is not None])

    def __scrapper_beacon(self):
        beacon_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        beacon_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        beacon_socket.bind(("", BEACON_PORT))
        while True:
            info, addr = beacon_socket.recvfrom(1024)
            if info.startswith(b"PING"):
                beacon_socket.sendto("PONG".encode(), addr)


    # Chord pregunta por el scrapper
    # Saber donde se inicializa el socket push