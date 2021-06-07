import logging
import zmq.sugar as zmq
from ..utils.tools import zpipe, recieve_multipart_timeout
from ..utils.const import CHORD_SCRAP_ADDR, REP_IPC, REQ_IPC


class Scrapper:
    def __init__(self) -> None:
        self.ctx = zmq.Context()
        
        # inproc
        self.pipe = zpipe(self.ctx)

        # debbug & info
        logging.basicConfig(format = "scrapper: %(levelname)s: %(message)s", level=logging.INFO)
        self.logger = logging.getLogger("chord")

    def __associate_to_chord(self):
        sock = self.ctx.socket(zmq.DEALER)
        sock.connect(CHORD_SCRAP_ADDR)
        sock.send_multipart([REQ_IPC, b""])
        rep = recieve_multipart_timeout(sock, 3000)
        if len(rep) == 0 or rep[0] != REP_IPC:
            return None
        return sock

    def run(self) -> None:
        iproc_sock =  self.__associate_to_chord()
        if iproc_sock is None:
            self.logger.error("Could not establish comunication with chord node. Exiting ...")
            return
        
        # TODO: Recieve requests and send
        # TODO: Parse Requests
        # TODO: Adapt Chord Node to comunicate with scrapper
        # ?: Use dealer to dealer, and just one scrapper per chord node, or
        # ?: use push-pull and a chord node may have multiple scrappers (locally),
        # ?: Maybe scrappers can have mutliple nodes too, but it gets a litlle complicated
        # ?: can no longer use push-pull pattern, it may be required router-router

