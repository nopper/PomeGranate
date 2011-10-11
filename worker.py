#!/usr/bin/env python

import imp
import sys
import json
import time

from mpi4py import MPI

from message import *
from utils import Logger, load_module

from mapper import Mapper

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
            self.comm.send(Message(MSG_AVAILABLE, None), dest=0)
            msg = self.comm.recv()

            if msg.command == MSG_COMPUTE_MAP:
                result = self.mapper.execute(msg.result)
                self.comm.send(Message(MSG_FINISHED, result), dest=0)

            elif msg.command == MSG_COMPUTE_REDUCE:
                result = self.reducer.execute(msg.result)
                self.comm.send(Message(MSG_FINISHED, result), dest=0)

            elif msg.command == MSG_SLEEP:
                time.sleep(msg.result)

            elif msg.command == MSG_QUIT:
                finished = True
                #self.info("Finished")

        self.comm.Barrier()

    def extract_cls(self, mname, fname):
        module = load_module(mname)
        return getattr(module, fname)

if __name__ == "__main__":
    Worker(sys.argv[1])
