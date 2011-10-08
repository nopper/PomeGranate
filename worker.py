#!/usr/bin/env python

import imp
import sys
import time

from mpi4py import MPI

from message import *
from utils import Logger
from dispatcher import Message

class Worker(Logger):
    def __init__(self, mapfile, reducefile):
        super(Worker, self).__init__("Worker")

        finished = False
        self.comm = MPI.COMM_WORLD.Get_parent()

        self.mapf    = self.extract_fun(mapfile, 'map')
        self.reducef = self.extract_fun(reducefile, 'reduce')

        while not finished:
            self.comm.send(Message(MSG_AVAILABLE, None), dest=0)
            msg = self.comm.recv()

            if msg.command == MSG_COMPUTE_MAP:
                key, value = msg.result
                result = self.mapf(key, value)

                self.comm.send(Message(MSG_FINISHED, result), dest=0)

            elif msg.command == MSG_COMPUTE_REDUCE:
                key, list = msg.result
                result = self.reducef(key, list)

                self.comm.send(Message(MSG_FINISHED, result), dest=0)

            elif msg.command == MSG_SLEEP:
                time.sleep(msg.result)

            elif msg.command == MSG_QUIT:
                finished = True
                #self.info("Finished")

        self.comm.Barrier()

    def extract_fun(self, mname, fname):
        mfile, mpath, mdesc = imp.find_module(mname)
        module = imp.load_module(mname, mfile, mpath, mdesc)
        return getattr(module, fname)

if __name__ == "__main__":
    Worker(sys.argv[1], sys.argv[2])
