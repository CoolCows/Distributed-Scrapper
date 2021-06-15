from hashlib import sha1
import sys
import time
import logging
from random import randint
from threading import Lock, Thread
from utils.tools import get_id, get_source_ip
from sortedcontainers import SortedSet

import zmq.sugar as zmq

from .utils import FunctionCall, finger_table_to_str, in_between


class DataStorage:
    def __init__(self, m) -> None:
        """
        Handles data storage in the form of keys-value pairs
        """
        self._dict = {}
        self.m = m

    def insert_pair(self, key, value):
        """
        Insert a pair key-value
        """
        self._dict[key] = value

    def insert_pairs(self, pairs):
        """
        Insert an iterable of key-value pairs
        """
        for pair in pairs:
            self.insert_pair(pair)

    def get_key(self, key):
        """
        Returns key-value pair associated to key
        """
        return (key, self._dict[key])

    def pop_key(self, key):
        """
        Returns and remove key-value pair associated to key
        """
        return (key, self._dict.pop(key))

    def get_interval_keys(self, lwb, inclusive_lower, upb, inclusive_upper):
        """
        Returns all key-value pairs associated with all keys belonging
        to the interval specified

        Parameters
        ----------
        lwb: int
            Lower bound of the interval
        inclusive_lower: bool
            Specified if lower bound is inclusive
        upb: int
            Upper bound of the interval
        inclusive_upper: bool
            Specified if upper bound is inclusive

        Returns
        import logging
        -------
        A list of key-value pairs (list could be empty)
        """
        keys_in_interval = []
        for key in self._dict.keys():
            if in_between(
                self.m,
                key,
                lwb=lwb,
                lequal=inclusive_lower,
                upb=upb,
                requal=inclusive_upper,
            ):
                keys_in_interval.append(key)

        return [(key, self._dict[key]) for key in keys_in_interval]

    def pop_interval_keys(self, lwb, inclusive_lower, upb, inclusive_upper):
        """
        Returns and remove all key-value pairs associated with all keys belonging
        to the interval specified

        Parameters
        ----------
        lwb: int
            Lower bound of the interval
        inclusive_lower: bool
            Specified if lower bound is inclusive
        upb: int
            Upper bound of the interval
        inclusive_upper: bool
            Specified if upper bound is inclusive

        Returns
        -------
        A list of key-value pairs (list could be empty)
        """
        keys_in_interval = []
        for key in self._dict.keys():
            if in_between(
                self.m,
                key,
                lwb=lwb,
                lequal=inclusive_lower,
                upb=upb,
                requal=inclusive_upper,
            ):
                keys_in_interval.append(key)

        ret = [(key, self._dict[key]) for key in keys_in_interval]
        for key in keys_in_interval:
            self._dict.pop(key)
        return ret

    def get_all(self):
        """
        Returns all key-value pairs
        """
        keys = self._dict.keys()
        return [(key, self._dict[key]) for key in keys]

    def pop_all(self):
        """
        Returns and removes all key-value pairs
        """
        keys = self.get_all()
        self._dict = {}
        return keys

class ChordNode:
    def __init__(self, m, port) -> None:
        self.address = (get_source_ip(), port)
        self.context = zmq.Context()
        self.reply = self.context.socket(zmq.REP)
        self.reply.bind("tcp://*:%s" % port)

        self.bits = m
        self.node_id = get_id(f"{self.address[0]}:{self.address[1]}") % 2**self.bits
        self.finger = [None for _ in range(m + 1)]
        self.successor_list = SortedSet(
            [],
            key=lambda n: n[0] - self.node_id
            if n[0] >= self.node_id
            else 2 ** self.bits - self.node_id + n[0],
        )
        self.storage = DataStorage(self.bits)
        self.pred_replica = DataStorage(self.bits)

        self.succ_list_lock = Lock()
        
        logging.basicConfig(format = "chordnode: %(levelname)s: %(message)s", level=logging.INFO)
        self.logger = logging.getLogger("chordnode")

    def rpc(self, node, funct_name, params=None, timeout=4000):
        """Remote Procedure Call between Chord's nodes. This method enables the
        communication between nodes in the ring.

        Parameters
        ----------
        node: tuple
            A tuple containing the node id and ip address where the function is
            going to be executed
        funct_name: str
            Name of the function to be executed remotely
        params: list
            Parameters
        timeout: int
            Maximun time spent waiting for answer

        Returns
        -------
        None: If the procedure to be executed return None, there was a timeout
        or node is None.
        """
        if node is None:
            return None

        node_id, address = node
        ip, port = address

        if params is None:
            params = []
        if address == self.address:
            return getattr(self, funct_name)(*params)

        request = self.context.socket(zmq.REQ)
        request.connect(f"tcp://{ip}:{port}")
        self.logger.info(f"Sending RPC '{funct_name} {params}' to node {node_id}...")
        
        request.send_pyobj(FunctionCall(funct_name, params))

        poller = zmq.Poller()
        poller.register(request, zmq.POLLIN)

        ready = dict(poller.poll(timeout))
        if ready:
            ret = request.recv_pyobj()
        else:
            self.logger.warning("Request for RPC timeout :(")
            ret = None
        request.close()
        return ret

    def _inbetween(self, key, lwb, lequal, upb, requal):
        return in_between(self.bits, key, lwb, lequal, upb, requal)

    def finger_table(self):
        """
        Returns finger table as str
        """
        return finger_table_to_str(self.node_id, self.finger)

    def is_online(self):
        """
        Returns if current node is online
        """
        return True

    def finger_start(self, i):
        """
        Returns the id of the start of the interval corresponding to the ith-finger
        """
        return (self.node_id + 2 ** (i - 1)) % (2 ** self.bits)

    def set_predecessor(self, n):
        """
        Sets n as the predecessor of the current node
        """
        self.finger[0] = n

    def predecessor(self):
        """
        Return the predecessor of the current node
        """
        if not self.rpc(self.finger[0], "is_online"):
            self.finger[0] = None
        return self.finger[0]

    def successor(self):
        """
        Return the successor of the current node
        """
        return self.finger[1]

    def find_successor(self, i):
        """
        Find sucessor of node i
        """
        n = self.find_predecessor(i)
        n_successor = self.rpc(n, "successor")
        return n_successor

    def find_predecessor(self, i):
        """
        Find predecessor of node i
        """
        n = (self.node_id, self.address)
        n_successor = self.finger[1]

        if n_successor is None:
            return

        while not self._inbetween(i, n[0], False, n_successor[0], True):
            n = self.rpc(n, "closest_preceding_finger", [i])
            n_successor = self.rpc(n, "successor")
            if n is None or n_successor is None:
                return
        return n

    def closest_preceding_finger(self, i):
        """
        Returns closest finger preceding i
        """
        for k in reversed(range(1, self.bits + 1)):
            key = self.finger[k]
            if key is not None and self._inbetween(
                key[0], self.node_id, False, i, False
            ):
                return self.finger[k]
        return self.node_id, self.address

    def join(self, idx=None, address=None):
        """
        Ask current node to join the ring

        Parameters
        ----------
        idx: int
            Id of the node to join
        address: str
            IP address of the node to join
        """
        if idx is not None:
            self.finger[0] = None
            self.finger[1] = self.rpc((idx, address), "find_successor", [self.node_id])
            # self.add_node(self.rpc(self.successor(), "successor"))
        else:
            for i in range(0, self.bits + 1):
                self.finger[i] = (self.node_id, self.address)

    def find_key(self, key):
        """
        Finds and returns key-value pair from Chord's ring
        """
        successor = self.find_successor(key)
        return self.rpc(successor, "get_key", [key])

    def get_key(self, key):
        """
        Returns key-value pair from local storage
        """
        return self.storage.get_key(key)

    def insert_key(self, key_value: tuple):
        """
        Insert key-value pair in Chord's ring
        """
        successor = self.find_successor(key_value[0])
        self.rpc(successor, "insert_keys_locally", [[key_value]])

    def insert_keys_locally(self, keys_values: list):
        """
        Insert key-value pairs in current node storage
        """
        for pair in keys_values:
            self.storage.insert_pair(*pair)

    def remove_key(self, key):
        """
        Remove key from Chord's ring
        """
        successor = self.find_successor(key)
        self.rpc(successor, "remove_keys_locally", [[key]])

    def remove_keys_locally(self, keys: list):
        """
        Remove key from current node storage
        """
        for key in keys:
            try:
                self.storage.pop_key(key)
            except KeyError:
                pass

    def pop_interval_keys(
        self, lwb: int, inclusive_lower: bool, upb: int, inclusive_upb: bool
    ):
        """
        Returns and remove all key-value pairs associated with all keys belonging
        to the interval specified
        """
        return self.storage.pop_interval_keys(lwb, inclusive_lower, upb, inclusive_upb)

    def get_interval_keys(
        self, lwb: int, inclusive_lower: bool, upb: int, inclusive_upb: bool
    ):
        """
        Returns all key-value pairs associated with all keys belonging
        to the interval specified
        """
        return self.storage.get_interval_keys(lwb, inclusive_lower, upb, inclusive_upb)

    def stabilize(self):
        """
        Periodically verify n's immediate successor and tell the successor about n
        """
        successor = self.successor()
        while not self.rpc(successor, "is_online", timeout=1000):
            if not self.successor_list:
                self.finger[1] = (self.node_id, self.address)
                return
            successor = self.finger[1] = self.pop_node(0)

        x = self.rpc(successor, "predecessor")
        if x is not None and self._inbetween(
            x[0], self.node_id, False, successor[0], False
        ):
            self.finger[1] = x
        self.rpc(self.successor(), "notify", [(self.node_id, self.address)])

        # Move keys
        lwb = self.predecessor()
        if lwb is not None:
            new_keys = self.rpc(
                node=self.successor(),
                funct_name="pop_interval_keys",
                params=[lwb[0], False, self.node_id, True],
            )
            if new_keys is not None:
                self.insert_keys_locally(new_keys)

        self.logger.info(self.finger_table())
        self.logger.info(self.storage._dict.keys())

    def notify(self, n):
        """
        Notify current node that his posible predecessor is node n
        """
        predecessor = self.predecessor()
        if predecessor is None or self._inbetween(
            n[0], predecessor[0], False, self.node_id, False
        ):
            self.set_predecessor(n)

    def update_pred_replica(self, keys):
        """
        Update predecessor's replica
        """
        for pair in keys:
            self.pred_replica.insert_pair(*pair)

    def send_data_replica(self):
        """
        Sends a replica of current keys to node's successor
        """
        successor = self.successor()

        if successor is None or successor[0] == self.node_id:
            return

        predecessor = self.predecessor()
        upb = self.node_id
        if predecessor is None:
            lwb = upb
        else:
            lwb = predecessor[0]

        keys = self.storage.get_interval_keys(lwb, False, upb, True)
        self.rpc(successor, "update_pred_replica", [keys])

    def update_storage(self):
        """
        Checks for predecessor status and in case is unknown, moves predecessor's replica
        to local storage. It also removes all keys stored in local storage than does not
        belong to current node.
        """
        predecessor = self.predecessor()
        if predecessor is None or predecessor[0] == self.node_id:
            pred_keys = self.pred_replica.pop_all()
            self.insert_keys_locally(pred_keys)
        elif predecessor[0] != self.node_id:
            self.storage.pop_interval_keys(self.node_id, False, predecessor[0], True)

    def fix_fingers(self):
        """
        Fixes a random entry in the finger table
        """
        i = randint(2, self.bits)
        self.finger[i] = self.find_successor(self.finger_start(i))

    def update_successor_list(self):
        """
        Update the alternative successor list
        """
        next_succ = None
        if len(self.successor_list) == 0:
            next_succ = self.rpc((self.node_id, self.address), "successor")

        elif len(self.successor_list) <= self.bits + 1:
            next_succ = self.successor_list[-1]
            next_succ = self.rpc(next_succ, "successor")

        if next_succ is not None and next_succ[0] != self.node_id:
            self.add_node(next_succ)

    def add_node(self, node):
        """
        Adds node to successors list
        """
        self.succ_list_lock.acquire()
        self.successor_list.add(node)
        self.succ_list_lock.release()

    def pop_node(self, index):
        """
        Returns and remove node from successors list
        """
        self.succ_list_lock.acquire()
        n = self.successor_list.pop(index)
        self.succ_list_lock.release()
        return n

    def run(self):
        """
        Main routine of chord node
        """

        self.start_chord_functionality()

        while True:
            fun = self.reply.recv_pyobj()
            try:
                funct = getattr(self, fun.name)
                ret = funct(*fun.params)
                self.logger.info(f"Sending reply for {funct}...")
                self.reply.send_pyobj(ret)
            except AttributeError:
                self.logger.warning(f"Request {funct} unknown")

    def start_chord_functionality(self):
        def stabilization():
            while True:
                time.sleep(1)
                self.stabilize()
                self.fix_fingers()

        def update_successor_list():
            while True:
                time.sleep(1)
                self.update_successor_list()

        def update_data():
            while True:
                time.sleep(2)
                self.send_data_replica()
                self.update_storage()

        Thread(target=stabilization, daemon=True).start()
        Thread(target=update_successor_list, daemon=True).start()
        Thread(target=update_data, daemon=True).start()


def main():
    m = 8
    ip = "127.0.0.1"
    join_to = -1
    if len(sys.argv) == 3:
        idx, port1 = sys.argv[1], sys.argv[2]
    elif len(sys.argv) == 5:
        idx, port1 = sys.argv[1], sys.argv[2]
        join_to, port2 = sys.argv[3], sys.argv[4]

    n = ChordNode(int(idx), m, ip, port1)
    print(n._inbetween(4, 3, False, 3, False))
    if join_to != -1:
        n.join(int(join_to), (ip, port2))
    else:
        n.join()
    n.run()
