#!/usr/bin/env python

"""
Generic module holding the overall general behavior of the generic worker that
can be specialized on demand depending on the received message.
"""

import sys
import json
import time

from mpi4py import MPI

from pomegranate.message import *
from pomegranate.utils import Logger, load_module

class Worker(Logger):
    def __init__(self, fconf):
        super(Worker, self).__init__("Worker")

        finished = False
        self.comm = MPI.COMM_WORLD.Get_parent()

        # Here we also need to handle the configuration file somehow
        conf = json.load(open(fconf))
        self.mapper = self.extract_cls(conf['map-module'], 'Mapper')(conf)
        self.reducer = self.extract_cls(conf['reduce-module'], 'Reducer')(conf)

        while not finished:
            self.comm.send(Message(MSG_AVAILABLE, 0, None), dest=0)
            msg = self.comm.recv()

            if msg.command == MSG_COMPUTE_MAP:
                info, result = self.mapper.execute(msg.result)

                msg = Message(MSG_FINISHED_MAP, msg.tag, result)
                msg.info = info

                self.info("Map performance: %.2f" % \
                           (info[0] / (1024 ** 2 * info[1])))

                self.comm.send(msg, dest=0)

            elif msg.command == MSG_COMPUTE_REDUCE:
                info, result = self.reducer.execute(msg.result)

                msg = Message(MSG_FINISHED_REDUCE, msg.tag, result)
                msg.info = info

                self.info("Reduce performance: %.2f" % \
                          (info[0] / (1024 ** 2 * info[1])))

                self.comm.send(msg, dest=0)

            elif msg.command == MSG_SLEEP:
                time.sleep(msg.result)

            elif msg.command == MSG_QUIT:
                finished = True

    def extract_cls(self, mname, fname):
        module = load_module(mname)
        return getattr(module, fname)

if __name__ == "__main__":
    Worker(sys.argv[1])
