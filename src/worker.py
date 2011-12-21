#!/usr/bin/env python

"""
Generic module holding the overall general behavior of the generic worker that
can be specialized on demand depending on the received message.
"""

import sys
import json
import time
import shutil
import os.path

from mpi4py import MPI

from pomegranate.message import *
from pomegranate.utils import Logger, load_module

try:
    from filesystem import Filesystem
    DFS_AVAILABLE = True
except ImportError:
    DFS_AVAILABLE = False

class Worker(Logger):
    def __init__(self, fconf, master_id, worker_id):
        super(Worker, self).__init__("Worker")

        finished = False
        self.comm = MPI.COMM_WORLD.Get_parent()

        # Here we also need to handle the configuration file somehow
        self.conf = conf = json.load(open(fconf))
        self.use_dfs = use_dfs = conf['dfs-enabled']

        self.datadir = self.conf['datadir']
        self.input_prefix = self.conf['input-prefix']
        self.output_prefix = self.conf['output-prefix']

        if use_dfs and not DFS_AVAILABLE:
            raise Exception("You need to install fsdfs in order to use the" \
                            " distributed mode. Otherwise just toggle it "  \
                            "off from the configuration file")
        elif use_dfs:
            dconf = conf['dfs-conf']

            host = '%s:%d' % (conf['dfs-host'],
                              conf['dfs-startport'] + int(worker_id))

            self.datadir = os.path.join(
                self.datadir,
                'master-{:06d}'.format(int(master_id)),
                'worker-{:06d}'.format(int(worker_id))
            )
            self.info("Creating directory structure in %s" % self.datadir)

            if os.path.exists(self.datadir):
                shutil.rmtree(self.datadir)

            os.makedirs(self.datadir)
            os.makedirs(os.path.join(self.datadir, self.input_prefix))
            os.makedirs(os.path.join(self.datadir, self.output_prefix))

            dconf['host'] = host
            dconf['datadir'] = self.datadir

            self.info('Starting DFS client on %s' % host)

            self.fs = Filesystem(dconf)
            self.fs.start()

        # Here we need somehow to override the default scheme in case of DFS
        conf['datadir'] = self.datadir

        self.master_id = int(master_id)
        self.worker_id = int(worker_id)

        self.mapper = self.extract_cls(conf['map-module'], 'Mapper')(conf)
        self.reducer = self.extract_cls(conf['reduce-module'], 'Reducer')(conf)

        # We provide a VFS abstraction
        self.mapper.setup(self)
        self.reducer.setup(self)

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

        if self.use_dfs:
            self.info("Stopping DFS client")
            self.fs.stop()
            self.info("Stopped")

    def extract_cls(self, mname, fname):
        module = load_module(mname)
        return getattr(module, fname)

    def pull_remote_files(self, reduce_idx, file_ids):
        if not self.use_dfs:
            return

        for fileid in file_ids:
            fname = "output-r{:06d}-p{:018d}".format(reduce_idx, int(fileid))
            fname = os.path.join(self.output_prefix, fname)

            full_path = os.path.join(self.datadir, fname)
            self.info("Checking %s" % full_path)

            if os.path.exists(full_path):
                self.info("Skipping %s. It is already present" % fname)
                continue

            self.info("Worker worker_id=%d is downloading file '%s'" % \
                      (self.worker_id, fname))

            downloaded = self.fs.downloadFile(fname)

            if not downloaded:
                self.info("Failed to download %s" % fname)

    def pull_remote_file(self, inp):
        filename, fileid = inp

        if self.use_dfs:
            self.info("Worker worker_id=%d is downloading file '%s'" % \
                      (self.worker_id, filename))
            self.fs.downloadFile(filename)

        return (os.path.join(self.datadir, filename), fileid)

    def push_local_file(self, fname):
        if not self.use_dfs:
            return

        fname = os.path.join(self.output_prefix, fname)
        self.info("Pushing file '%s' into global DFS" % fname)
        self.fs.importFile(os.path.join(self.datadir, fname), fname)

if __name__ == "__main__":
    # Arguments are: <configuration file> <master-id> <worker-id>
    Worker(sys.argv[1], sys.argv[2], sys.argv[3])
