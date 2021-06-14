import zmq.sugar as zmq
import time
import socket
from typing import List, Tuple, Union
from ..utils.const import BEACON_PORT

def get_source_ip():
    import subprocess
    # address = subprocess.check_output(["hostname", "-s", "-I"]).decode("utf-8")[:-2]
    address = subprocess.check_output(["hostname", "-i"]).decode("utf-8")[:-1]
    return address

def zpipe(ctx):
    import binascii
    import os

    a = ctx.socket(zmq.PAIR)
    b = ctx.socket(zmq.PAIR)
    a.linger = b.linger = 0
    a.hwm = b.hwm = 1
    iface = "inproc://%s" % binascii.hexlify(os.urandom(8))
    a.bind(iface)
    b.connect(iface)
    return a, b

BEACON_PORT = 8001
def net_beacon(ip, id_bits):
    beacon_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    beacon_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    beacon_socket.bind(("", BEACON_PORT))
    while True:
        info, addr = beacon_socket.recvfrom(1024)
        if addr[0] == ip:
            continue
        if info.startswith(b"PING"):
            _, bits = info.split(b'@')
            bits = int.from_bytes(bits, "big")
            if bits == id_bits:
                beacon_socket.sendto("PONG".encode(), addr)

def find_nodes(id_bits) -> str:
    print("Searching for on-line nodes ...")
    broadcast_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    broadcast_socket.settimeout(0.5)
    tolerance = 3
    while tolerance > 0:
        broadcast_socket.sendto(
            b"PING@" + int.to_bytes(id_bits, 8, "big"),
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
        address = f"{address[0]}:{address[1]}"
    router.connect_rid = address.encode()
    router.connect(f"tcp://{address}")
 
def recv_from_router(router:zmq.Socket, address:str, timeout_sec:int = 1) -> Tuple:
    message = ()
    pending = []
    start = time.time()
    while time.time() - start < timeout_sec:
        rep = recieve_multipart_timeout(router, timeout_sec)
        if len(rep) == 0:
            break
        idx, *_ = rep
        if idx.decode() == address:
            message = (idx, *_)
            break
        else:
            pending.append((idx, *_))
    return message, pending

def register_socks(poller:zmq.Poller, *sockets) -> None:
    for sock in sockets:
        poller.register(sock, zmq.POLLIN)

def parse_address(address) -> Tuple[str, int]:
    if isinstance(address, bytes):
        address = address.decode()

    ip_addr, ip_port = address.split(":")
    return ip_addr, int(ip_port)