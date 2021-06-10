import zmq.sugar as zmq
from utils import FunctionCall


class ChordClient:
    def __init__(self) -> None:
        self.context = zmq.Context()

    def run(self):
        while True:
            try:
                print("Enter key")
                print("----------------------------------")
                key = input()
                print("Enter node's address")
                print("----------------------------------")
                address = input()
                ip, port = address.split(":")
                print(f"Sending LOOKUP request for key {key} to node in {address}")
                request = self.context.socket(zmq.REQ)
                request.connect(f"tcp://{ip}:{port}")
                request.send_pyobj(FunctionCall("find_successor", [int(key)]))

                poller = zmq.Poller()
                poller.register(request, zmq.POLLIN)

                ready = dict(poller.poll(5000))
                if ready:
                    ret = request.recv_pyobj()
                    print(ret)
                else:
                    print("LOOKUP request timeout :(")
                request.close()

            except KeyboardInterrupt:
                self.context.destroy()
                exit()


client = ChordClient()
client.run()