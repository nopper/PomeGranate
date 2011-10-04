#!/usr/bin/env python

from mpi4py import MPI

from utils import Logger

class Worker(Logger):
    def __init__(self):
        super(Worker, self).__init__("Worker")

        self.info("Hello there")

if __name__ == "__main__":
    Worker()
