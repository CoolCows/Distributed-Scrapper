import zmq.sugar as zmq
import sys
from random import randint
from threading import Thread
import time
from .utils import FunctionCall, finger_table_to_str, in_between


class DataStorage:
    """
    Handles data store in the form of keys-value pairs
    """

    def __init__(self, m) -> None:
        self._dict = {}
        self.m = m

    def insert_pair(self, key, value):
        """
        Insert a piar key-value
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
        -------
        A list of key-value pairs (list could be empty)
        """
        keys_in_interval = []
        for key in self._dict.keys:
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
        for key in self._dict.keys:
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


class ChordNode:
    def __init__(self, idx, m, ip, port) -> None:
        self.address = (ip, port)
        self.context = zmq.Context()
        self.reply = self.context.socket(zmq.REP)
        self.reply.bind("tcp://*:%s" % port)

        self.m = m
        self.node_id = idx
        self.finger = [None for i in range(m + 1)]
        self.successor_list = [None]
        self.storage = DataStorage(self.m)

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
        request.send_pyobj(FunctionCall(funct_name, params))

        poller = zmq.Poller()
        poller.register(request, zmq.POLLIN)

        ready = dict(poller.poll(timeout))
        if ready:
            ret = request.recv_pyobj()
        else:
            ret = None
        request.close()
        return ret

    def _inbetween(self, key, lwb, lequal, upb, requal):
        return in_between(self.m, key, lwb, lequal, upb, requal)

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
        return (self.node_id + 2 ** (i - 1)) % (2 ** self.m)

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
        for k in reversed(range(1, self.m + 1)):
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
            self.successor_list[0] = self.rpc(self.successor(), "successor")
        else:
            for i in range(0, self.m + 1):
                self.finger[i] = (self.node_id, self.address)

    def stabilize(self):
        """
        Periodically verify n's immediate successor and tell the successor about n
        """
        successor = self.successor()
        while not self.rpc(successor, "is_online", timeout=1000):
            if not self.successor_list:
                self.finger[1] = (self.node_id, self.address)
                return
            successor = self.finger[1] = self.successor_list.pop(0)

        x = self.rpc(successor, "predecessor")
        if x is not None and self._inbetween(
            x[0], self.node_id, False, successor[0], False
        ):
            self.finger[1] = x
        self.rpc(self.successor(), "notify", [(self.node_id, self.address)])

        # TODO: Move keys ...

    def notify(self, n):
        """
        Notify current node that his posible predecessor is node n
        """
        if self.predecessor() is None or self._inbetween(
            n[0], self.predecessor()[0], False, self.node_id, False
        ):
            self.set_predecessor(n)

    def fix_fingers(self):
        """
        Fixes a random entry in the finger table
        """
        i = randint(2, self.m)
        self.finger[i] = self.find_successor(self.finger_start(i))

    def update_successor_list(self):
        """
        Update the alternative successor list
        """
        if not self.successor_list:
            self.successor_list.append(self.rpc(self.successor(), "successor"))
        elif len(self.successor_list <= self.m):
            last_alt_succ = self.successor_list[-1]
            self.successor_list.append(self.rpc(last_alt_succ, "successor"))

        # TODO: Update entire list on each call

    def run(self):
        """
        Main routine of chord node
        """

        def stabilization():
            while True:
                time.sleep(1)
                self.stabilize()
                self.fix_fingers()

        def update_successorList():
            while True:
                time.sleep(1)
                self.update_successor_list

        Thread(target=stabilization, daemon=True).start()
        Thread(target=update_successorList, daemon=True).start()

        while True:
            print(self.finger_table())
            fun = self.reply.recv_pyobj()
            print(fun)
            try:
                funct = getattr(self, fun.name)
                ret = funct(*fun.params)
                self.reply.send_pyobj(ret)
            except AttributeError:
                pass


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


if __name__ == "__main__":
    main()
