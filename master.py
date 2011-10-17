import imp
import sys
import json

from mpi4py import MPI

from mux import Muxer
from utils import Logger, load_module
from message import *
from dispatcher import *


class Master(Logger):
    def __init__(self, fconf):
        super(Master, self).__init__("Master")

        self.fconf = fconf
        self.conf = json.load(open(fconf))

        self.comm = MPI.COMM_WORLD
        self.n_machines = self.__count_machines()

        self.communicators = Muxer()

        self.info("We have %d available slots" % (self.n_machines))

    def __count_machines(self):
        """
        Read the number of MPI slots that we can possibly use
        @return an integer indicating the number of available MPI slots
        """
        count = 0

        for line in open(self.conf["machine-file"]).readlines():
            line = line.strip()

            if line[0] == '#':
                continue

            try:
                # Extract the number from a string like
                # host.domain:2
                count += int(line.rsplit(':', 1)[1])
            except Exception, exc:
                count += 1

        return count

    def run(self):
        self.__main_loop()

    def __main_loop(self):
        # Try to find the number of processing element trying to maximize it
        num_machines = min(self.n_machines,
                           max(self.conf['num-mapper'],
                               self.conf['num-reducer']))

        self.info("We will use %d slots" % (num_machines))

        for i in range(num_machines):
            comm = MPI.COMM_SELF.Spawn(sys.executable,
                                       args=["worker.py", self.fconf],
                                       maxprocs=1)
            #comm.Set_errhandler()
            self.communicators.add_channel(comm)

        self.worker_status = [None, ] * num_machines
        self.dispatcher = WorkDispatcher(self.input())

        while not self.dispatcher.finished():
            idx, comm = self.communicators.receive()

            msg = comm.recv()
            # TODO :error .. remember to reduce reducers
            # In case we have a failure we have to decrement the number of
            # reducer otherwise is counted twice. Probably the same applies for
            # map

            if msg.command == MSG_AVAILABLE:
                self.debug("Worker %d is available for a new task" % idx)
                finished = self.__assign_work(idx, comm)

            elif msg.command == MSG_FINISHED:
                # In this case the map has finished computing, therefore we
                # have to put the result in the "job queue" of the dispatcher.
                if self.worker_status[idx].type == TYPE_MAP:
                    ret = self.on_map_finished(msg.result)

                    if ret is not None:
                        self.dispatcher.push_work(
                            WorkerStatus(TYPE_REDUCE, ret)
                        )

                    self.dispatcher.map_finished()

                elif self.worker_status[idx].type == TYPE_REDUCE:
                    self.on_reduce_finished(msg)
                    self.dispatcher.reduce_finished()

                self.worker_status[idx] = None

        for idx, comm in enumerate(self.communicators.channels):
            comm.send(Message(MSG_QUIT, None), dest=0)
            comm.Barrier()

        self.info("Waiting on the final barrier")

    def __assign_work(self, idx, comm):
        """
        Assign a job to a generic worker
        """

        wstatus = self.dispatcher.pop_work()

        if wstatus is None:
            return True

        self.worker_status[idx] = wstatus

        if wstatus.type == TYPE_MAP:
            msg = MSG_COMPUTE_MAP
        elif wstatus.type == TYPE_REDUCE:
            msg = MSG_COMPUTE_REDUCE
        elif wstatus.type == TYPE_DUMMY:
            msg = MSG_SLEEP

        self.debug("Assigning %s as role to worker %d" % (MSG_TO_STR[msg], idx))

        comm.send(Message(msg, wstatus.state), dest=0)
        return False

    #
    # Public function that can be overriden
    #

    def on_map_finished(self, msg):
        new_status = WorkerStatus(TYPE_REDUCE, msg.result)

    def on_reduce_finished(self, msg):
        self.info("Final result %s" % str(msg.result))

    def input(self):
        raise Exception("Not implemented")

def start_mapreduce(mcls):
    if len(sys.argv) != 2:
        print("Usage: %s <conf-file>" % (sys.argv[0]))
        sys.exit(-1)

    if MPI.COMM_WORLD.Get_rank() == 0:
        mcls(sys.argv[1]).run()
