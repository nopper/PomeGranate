"""
This module holds the definition of the Multiplexer for a set of MPI
communicators.
"""

import sys
import time

from mpi4py import MPI
from threading import Lock

class Muxer(object):
    def __init__(self, unique_id, nproc, args, interval=0.1):
        """
        @param unique_id an unique ID identifying the current master
        @param nproc the number of MPI processes you want use
        @param args the arguments to pass to the MPI_Spawn
        @param interval the time you want to sleep before starting a new
               polling cycle of reads from the intercommunicators
        """
        self.unique_id = unique_id
        self.last_worker_id = -1

        self.index = 0
        self.channels = []
        self.interval = interval
        self.arguments = args

        self.lock = Lock()
        self.spawn_more(nproc)

    def spawn_more(self, nproc):
        """
        @param Increase the number of generic workers by nproc
        """
        new_channels = []

        arguments = list(self.arguments)
        arguments.append(str(self.unique_id))

        for i in range(nproc):
            self.last_worker_id += 1
            arguments.append(str(self.last_worker_id))

            comm = MPI.COMM_SELF.Spawn(sys.executable,
                                       args=arguments,
                                       maxprocs=1)
            arguments.pop()
            new_channels.append(comm)

        with self.lock:
            self.channels.extend(new_channels)

    def remove(self, comm):
        """
        Remove a given Intercommunicator from the listening set
        @param comm a MPI.Intercommunicator
        """
        with self.lock:
            self.channels.remove(comm)

    def get_total(self):
        "@return the number of MPI processes used"
        with self.lock:
            return len(self.channels)

    def send_all(self, msg):
        """
        Broadcast a given message to all the Intracommunicators
        @param msg the message you want to broadcast
        """
        with self.lock:
            for comm in self.channels:
                comm.send(msg, dest=0)

    def receive(self):
        """
        Implement a non-deterministic receive. If you want you can override
        this method providing other policies.

        This function will sleep if the cycle has not returned a communicator
        to read from.
        """
        temp = self.index

        while True:
            self.lock.acquire()
            self.index = (self.index + 1) % (len(self.channels))
            comm = self.channels[self.index]

            if comm.Iprobe():
                self.lock.release()
                return (self.index, comm)

            if self.index == temp:
                self.lock.release()
                time.sleep(self.interval)
            else:
                self.lock.release()
