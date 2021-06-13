import zmq.sugar as zmq
from .utils import FunctionCall


class ChordClient:
    def __init__(self) -> None:
        self.context = zmq.Context()

        self.comands_params = {
            "find_successor": self.single_key,
            "find_key": self.single_key,
            "insert_key": self.key_value,
            "remove_key": self.single_key,
            "finger_table": self.none,
        }

    def single_key(self):
        print("Enter key:")
        key = int(input())
        return [key]

    def key_value(self):
        print("Enter key and value:")
        key,value = input().split(" ")
        key = int(key)
        return [(key,value)]

    def none(self):
        return []

    def run(self):
        while True:
            try:
                print("Enter node's address")
                print("----------------------------------")
                address = input()
                ip, port = address.split(":")
                print("Enter request")
                print("----------------------------------")
                command = input()

                params = self.comands_params[command]()

                request = self.context.socket(zmq.REQ)
                request.connect(f"tcp://{ip}:{port}")
                request.send_pyobj(FunctionCall(command, params))

                poller = zmq.Poller()
                poller.register(request, zmq.POLLIN)

                ready = dict(poller.poll(5000))
                if ready:
                    ret = request.recv_pyobj()
                    print(ret)
                else:
                    print("Request timeout :(")
                request.close()

            except KeyboardInterrupt:
                self.context.destroy()
                exit()

