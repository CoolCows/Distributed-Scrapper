import logging
import time
import zmq.sugar as zmq
from threading import Lock, Thread
from .scrapper_const import *
from ..utils.tools import parse_address, zpipe, recieve_multipart_timeout, get_source_ip
from ..utils.const import REP_ASOC_SCRAP_ALR, REP_SCRAP_ACK, REP_SCRAP_URL, REQ_SCRAP_ACK, SCRAP_PORT, REQ_ASOC_SCRAP, REP_ASOC_SCRAP_YES, REP_ASOC_SCRAP_NO


class Scrapper:
    def __init__(self, max_threads:int) -> None:
        self.online = False
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

    def run(self) -> None:
        iproc_sock =  self.__associate_to_chord()
        if iproc_sock is None:
            self.logger.error("Could not establish comunication with chord node. Exiting ...")
            return


    def __communication_loop(self):
        comm_sock = self.ctx.socket(zmq.ROUTER)
        comm_sock.bind(f"tcp://{self.ip}:{SCRAP_PORT}")

        while self.online:
            request = recieve_multipart_timeout(comm_sock, TIMEOUT_COMM)
            if len(request) == 0:
                continue
            
            sock_id, flag, info = request
            if flag == REQ_ASOC_SCRAP:
                sock_addr = info.decode()
                self.__update_workers()
                if sock_addr in [val[0] for val in self.worker_threads]:
                    comm_sock.send_multipart([sock_id, REP_ASOC_SCRAP_ALR, b""])
                elif self.num_threads < self.max_threads:
                    self.create_worker(sock_addr)
                    comm_sock.send_multipart([sock_id, REP_ASOC_SCRAP_YES, b""])
                else:
                    comm_sock.send_multipart([sock_id, REP_ASOC_SCRAP_NO, b""])
            else:
                raise Exception(f"Unknown Flag recieved: {flag}")
        
        comm_sock.close()
             
    def create_worker(self, addr):
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
            # TODO: do work ...
            push_sock.send_multipart([REP_SCRAP_URL, b"worked"])
        
        pull_sock.close()
        push_sock.close()

    def __update_workers(self):
        self.worker_threads = [None for thread in self.worker_threads if thread is None or not thread[1].is_alive()]
        self.num_threads = len([thread for thread in self.worker_threads if thread is not None])

    # Chord pregunta por el scrapper
    # Saber donde se inicializa el socket push