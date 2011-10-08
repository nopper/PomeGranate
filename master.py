import imp
import sys
import json

from mpi4py import MPI

from mux import Muxer
from utils import Logger
from message import *
from dispatcher import *


class Master(Logger):
    def __init__(self, fconf):
        super(Master, self).__init__("Master")

        self.comm = MPI.COMM_WORLD
        self.conf = json.load(open(fconf))
        self.n_machines = self.count_machines()

        self.communicators = Muxer()

        self.info("We have %d available slots" % (self.n_machines))

    def count_machines(self):
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

    def main_loop(self):
        # Try to find the number of processing element trying to maximize it
        num_machines = min(self.n_machines,
                           max(self.conf['num-mapper'],
                               self.conf['num-reducer']))

        self.info("We will use %d slots" % (num_machines))

        mname = self.conf['input-module']
        mfile, mpath, mdesc = imp.find_module(mname)
        module = imp.load_module(mname, mfile, mpath, mdesc)

        mapfile = self.conf['map-module']
        reducefile = self.conf['reduce-module']

        for i in range(num_machines):
            comm = MPI.COMM_SELF.Spawn(sys.executable,
                                       args=["worker.py", mapfile, reducefile],
                                       maxprocs=1)
            #comm.Set_errhandler()
            self.communicators.add_channel(comm)

        # Let's create a generator to keep track of the inputs key,value and an
        # array to keep track of the state of each worker in case of faults, in
        # order to be able to spawn a replica.
        self.worker_status = [None, ] * num_machines
        self.dispatcher = WorkDispatcher(module.input())
        self.num_reducers = 0

        #finished = False

        # Lazy evaluation of the any therefore no problem.
        while self.num_reducers > 0 or not self.dispatcher.finished():
            idx, comm = self.communicators.receive()

            # TODO :error .. remember to reduce reducers
            msg = comm.recv()

            if msg.command == MSG_AVAILABLE:
                finished = self.assign_work(idx, comm)

            elif msg.command == MSG_FINISHED:
                # In this case the map has finished computing, therefore we
                # have to put the result in the "job queue" of the dispatcher.
                if self.worker_status[idx].type == TYPE_MAP:
                    new_status = WorkerStatus(TYPE_REDUCE, msg.result)
                    self.dispatcher.push_work(new_status)

                elif self.worker_status[idx].type == TYPE_REDUCE:
                    self.info("Final result %s" % str(msg.result))
                    self.num_reducers -= 1

                self.worker_status[idx] = None

        for idx, comm in enumerate(self.communicators.channels):
            comm.send(Message(MSG_QUIT, None), dest=0)
            comm.Barrier()

        self.info("Waiting on the final barrier")

    def assign_work(self, idx, comm):
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
            self.num_reducers += 1
        elif wstatus.type == TYPE_DUMMY:
            msg = MSG_SLEEP

        comm.send(Message(msg, wstatus.state), dest=0)
        return False




if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: %s <conf-file>" % (sys.argv[0]))
        sys.exit(-1)

    if MPI.COMM_WORLD.Get_rank() == 0:
        master = Master(sys.argv[1])
        master.main_loop()
