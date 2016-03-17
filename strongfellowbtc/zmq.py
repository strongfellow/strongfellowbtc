
from __future__ import absolute_import

import zmq

class socket():
    def __init__(self, port, topic):
        self._context = zmq.Context()
        self._port = port
        self._topic = topic


    def __enter__(self):
        socket = self._context.socket(zmq.SUB)
        socket.setsockopt(zmq.SUBSCRIBE, self._topic)
        socket.connect("tcp://127.0.0.1:%i" % self._port)
        return socket

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._context.destroy()
