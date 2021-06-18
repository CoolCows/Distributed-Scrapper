import logging
import pickle
from typing import Tuple
import zmq.sugar as zmq
from threading import Lock, Thread
from .scraper_const import *
from .scraper_tools import extract_html 
from utils.tools import get_router, net_beacon, zpipe, recieve_multipart_timeout, get_source_ip
from utils.const import CODE_WORD_SCRAP, REP_SCRAP_ACK_CONN, REP_SCRAP_ACK_NO_CONN, REP_SCRAP_URL, REQ_SCRAP_ACK, REQ_SCRAP_URL, SCRAP_BEACON_PORT, SCRAP_PORT, REQ_SCRAP_ASOC, REP_SCRAP_ASOC_YES, REP_SCRAP_ASOC_NO


class Scraper:
    def __init__(self, port, max_threads:int, visible:bool = False) -> None:
        self.online = False
        self.visible = visible

        self.max_threads = max_threads
        self.worker_threads = [None for _ in range(max_threads)]
        self.num_threads = 0
        
        self.ctx = zmq.Context()
        self.address = (get_source_ip(), port)
        
        # inproc
        self.pipe = zpipe(self.ctx)

        #sync
        self.lock = Lock()

        # debbug & info
        logging.basicConfig(format = "%(name)s: %(levelname)s: %(message)s", level=logging.DEBUG)
        self.logger = logging.getLogger("scraper")

    def run(self):
        self.logger.info(f"Scrapper running at {self.address[0]}:{self.address[1]}")
        self.online = True

        if self.visible:
            self.logger.debug("Scraper visble")
            Thread(
                target=net_beacon,
                args=(self.address[1], SCRAP_BEACON_PORT, CODE_WORD_SCRAP),
                daemon=True
            ).start()

        while self.online:
            # In case __communication_loop stops unexpectedly, it starts again
            self.__communication_loop()
            # try:
            #     self.__communication_loop()
            # except Exception:
            #     if isinstance(Exception, KeyboardInterrupt):
            #         self.online = False
            #     else:
            #         self.logger.error(f"Error in communication loop, restarting")
        self.logger.info("Waiting for worker threads to finish ...")

    def __communication_loop(self):
        comm_sock = get_router(self.ctx)
        comm_sock.bind(f"tcp://{self.address[0]}:{self.address[1]}")

        while self.online:
            request = recieve_multipart_timeout(comm_sock, TIMEOUT_COMM)
            if len(request) == 0:
                continue
            
            self.logger.debug(f"recieved request")
            sock_id, flag, info = request
            if flag == REQ_SCRAP_ASOC:
                sock_addr = pickle.loads(info)
                self.__update_workers()
                if self.num_threads < self.max_threads:
                    self.__create_worker(sock_addr)
                    self.logger.debug("Replying yes")
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
             
    def __create_worker(self, addr:Tuple):
        self.logger.debug("Creating worker")
        index = self.worker_threads.index(None)
        t = Thread(target=self.__work_loop, args=(addr, index))
        self.worker_threads[index] = (addr, t)
        self.num_threads += 1
        t.start()

    def __work_loop(self, addr, thread_id):
        ip, port = addr
        pull_sock = self.ctx.socket(zmq.PULL)
        push_sock = self.ctx.socket(zmq.PUSH)
        self.logger.debug(f"WorkerThread({thread_id}): connecting to {ip}: {port + 2}, {port + 3}")
        pull_sock.connect(f"tcp://{ip}:{port + 2}")
        push_sock.connect(f"tcp://{ip}:{port + 3}")

        pull_sock.rcvtimeo = (TIMEOUT_WORK * MAX_IDDLE) * 1000
        while True: 
            try:
                url = pull_sock.recv_pyobj()
            except zmq.error.Again:
                break
            html, urls = extract_html(url, self.logger)
            push_sock.send_pyobj((url, html, urls))
        
        self.logger.debug(f"WorkerThread({thread_id}): closing.")
        pull_sock.close()
        push_sock.close()

    def __update_workers(self):
        self.worker_threads = [None if thread is None or not thread[1].is_alive() else thread for thread in self.worker_threads]
        self.num_threads = len([thread for thread in self.worker_threads if thread is not None])

