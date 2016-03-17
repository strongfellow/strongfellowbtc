
import zmq

class socket():
    def __init__(self, port, topic):
        self._context = zmq.Context()


    def __enter__(self):
        socket = self._.socket(zmq.SUB)
        socket.setsockopt(zmq.SUBSCRIBE, topic)
        socket.connect("tcp://127.0.0.1:%i" % port)
        return socket

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._context.destroy()
    
def generate_zmq_messages(port, *topics):
    port = 28332
    zmqContext = zmq.Context()
    for t in topics:
        zmqSubSocket.setsockopt(zmq.SUBSCRIBE, t)
    zmqSubSocket.connect("tcp://127.0.0.1:%i" % port)


    try:
        while True:
            msg = zmqSubSocket.recv_multipart()
            topic = str(msg[0])
            body = msg[1]
            yield (topic, body)
        except:
            zmqContext.destroy()
            raise
