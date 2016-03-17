
from __future__ import absolute_import

import zmq

class socket():
    def __init__(self, port, topic):
        self._context = zmq.Context()


    def __enter__(self):
        socket = self._context.socket(zmq.SUB)
        socket.setsockopt(zmq.SUBSCRIBE, topic)
        socket.connect("tcp://127.0.0.1:%i" % port)
        return socket

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._context.destroy()
