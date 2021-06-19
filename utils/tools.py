from hashlib import sha1
from pickle import TRUE
from sortedcontainers.sortedset import SortedSet
import zmq.sugar as zmq
import time
import socket
from typing import List, NoReturn, Tuple, Union

from zmq.sugar.frame import Message

def get_source_ip():
    import subprocess
    address = subprocess.check_output(["hostname", "-s", "-I"]).decode("utf-8").split()[0]#[:-2]
    # address = subprocess.check_output(["hostname", "-i"]).decode("utf-8")[:-1]
    return address

def zpipe(ctx):
    import binascii
    import os

    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    # a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a, b

def net_beacon(node_port:int, beacon_port:int, code_word:str) -> NoReturn:
    port_byte = str(node_port).encode()
    beacon_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    beacon_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    beacon_socket.bind(("", beacon_port))
    while True:
        info, addr = beacon_socket.recvfrom(1024)
        if info == b"ping" + code_word:
            beacon_socket.sendto(b"pong" + code_word + b"@" + port_byte, addr)

def find_nodes(port:int, code_word:bytes, tolerance:int = 3, all:bool = False) -> List[Tuple[str, int]]:
    broadcast_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.settimeout(0.5)
    nodes = []
    while tolerance > 0:
        broadcast_socket.sendto(
            b"ping" + code_word,
            ("<broadcast>", port)
        )
        try:
            while True:
                info, addr = broadcast_socket.recvfrom(1024)
                if info.startswith(b"pong" + code_word):
                    tolerance = 0
                    node_port = int(info.split(b'@')[1].decode())
                    nodes.append((addr[0], node_port))
                    if not all:
                        break
        except socket.timeout:
            tolerance -= 1
    broadcast_socket.close()
    return nodes

def recieve_multipart_timeout(sock, timeout_sec):
    start = time.time()
    while time.time() -  start < timeout_sec:
        try:
            res = sock.recv_multipart(zmq.NOBLOCK)
            return res
        except zmq.error.Again:
            continue
    return []

def clean_pipeline(sock):
    while True:
        try:
            sock.recv_multipart(zmq.NOBLOCK)
        except zmq.error.Again:
            break

def get_router(ctx:zmq.Context) -> zmq.Socket:
    router = ctx.socket(zmq.ROUTER)
    router.router_mandatory = 1
    return router

def connect_router(router:zmq.Socket, address:Union[Tuple, str]) -> None:
    if isinstance(address, tuple):
        address = address_to_string(address)
    router.connect_rid = address.encode()
    router.connect(f"tcp://{address}")
 
def recv_from_router(router:zmq.Socket, address:str, timeout_sec:int = 1) -> Tuple:
    message = ()
    pending = []
    router.rcvtimeo = timeout_sec*1000
    while True:
        try:
            rep = router.recv_multipart()
        except zmq.error.Again:
            break
        idx, *other = rep
        if idx.decode() == address:
            message = (idx, *other)
            break
        else:
            pending.append((idx, *other))
    
    router.rcvtimeo = 0
    return message, pending

def register_socks(poller:zmq.Poller, *sockets) -> None:
    for sock in sockets:
        poller.register(sock, zmq.POLLIN)

def parse_address(address) -> Tuple[str, int]:
    if isinstance(address, bytes):
        address = address.decode()

    ip_addr, ip_port = address.split(":")
    return ip_addr, int(ip_port)

def address_to_string(address:Tuple[str, int]) -> str:
    return f"{address[0]}:{address[1]}"

def get_id(addr:str) -> int:
    hexhash = sha1(addr.encode()).hexdigest()
    return int(hexhash, 16)