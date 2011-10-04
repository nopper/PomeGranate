import imp
import sys
import json

from mpi4py import MPI
from utils import Logger

class Master(Logger):
    def __init__(self, fconf):
        super(Master, self).__init__("Master")

        self.comm = MPI.COMM_WORLD
        self.conf = json.load(open(fconf))
        self.n_machines = self.count_machines()

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

        mname = self.conf['input-module']
        mfile, mpath, mdesc = imp.find_module(mname, ".")
        module = imp.load_module(mname, mfile, mpath, mdesc)

        comm = MPI.COMM_SELF.Spawn(sys.executable, args=["worker.py"], maxprocs=num_machines)

        #for k, v in module.input():
        #for rank in xrange(self.conf["num-mapper"]):

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: %s <conf-file>" % (sys.argv[0]))
        sys.exit(-1)

    if MPI.COMM_WORLD.Get_rank() == 0:
        master = Master(sys.argv[1])
        master.main_loop()
