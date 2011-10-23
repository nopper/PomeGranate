import imp
import sys
import json
import time
import urllib2

from mpi4py import MPI

from mux import Muxer
from threading import Lock, Thread
from utils import Logger, load_module
from message import *

class Master(Logger):
    def __init__(self, nick, fconf):
        super(Master, self).__init__("Master")

        self.fconf = fconf
        self.conf = json.load(open(fconf))

        self.comm = MPI.COMM_WORLD
        self.n_machines = self.__count_machines()

        self.communicators = Muxer()

        self.info("We have %d available slots" % (self.n_machines))

        self.nick = nick
        self.url = self.conf['master-url']

        self.num_map = 0
        self.num_reducer = 0

        self.lock = Lock()
        self.map_queue = []
        self.reduce_queue = []
        self.dead_man_walking = False

        self.__register()

        self.ping_thread = Thread(target=self.__ping_thread)

        # This is for sending back to the manager the notifications about
        # completed works

        self.notifications = []
        self.notifications_lock = Lock()
        self.notify_thread = Thread(target=self.__notify_thread)

        self.notify_thread.start()
        self.ping_thread.start()


    ##########################################################################
    # Utility functions to send request to the manager
    ##########################################################################

    def __register(self):
        reply = self.__send_req('registration')
        reply.close()

        print reply

        if reply.code == 200:
            self.info("Master named %s succesfully registered" % self.nick)
        else:
            self.warning("Unable to register master named %s" % self.nick)

    def __send_req(self, type, nick=None, data=''):
        if nick is None:
            nick = self.nick

        data = json.dumps({'type': type, 'nick': nick, 'data': data})
        req = urllib2.Request(self.url, data, {'Content-Type': 'application/json'})
        self.debug("Request to %s - %s" % (self.url, data))
        return urllib2.urlopen(req)

    ##########################################################################
    # Threading
    ##########################################################################

    def __notify_thread(self):
        # TODO: maybe it is better to bulk notifications and to unlock between
        # sending messages.
        while not self.__finished():
            with self.notifications_lock:
                while self.notifications:
                    msg, reduce_pushed = self.notifications.pop(0)

                    if msg.command == MSG_FINISHED_MAP:
                        self.__send_req('map-completed',
                                        data=(msg.tag,
                                              reduce_pushed,
                                              msg.result))

                    elif msg.command == MSG_FINISHED_REDUCE:
                        self.__send_req('reduce-completed',
                                        data=(msg.tag, msg.result))

            time.sleep(1)

    def __ping_thread(self):
        while not self.__finished():
            with self.lock:
                reply = json.load(self.__send_req('work-request'))

                self.debug("New reply %s" % str(reply))

                reply_type = reply['type']

                if reply_type == 'plz-die':
                    self.dead_man_walking = True

                elif reply_type == 'try-later':
                    pass

                elif reply_type == 'compute-map':
                    tag = reply['nick']
                    work = reply['data']
                    self.__push_work(WorkerStatus(TYPE_MAP, tag, work))

                elif reply_type == 'compute-reduce':
                    tag = reply['nick']
                    work = reply['data']
                    self.__push_work(WorkerStatus(TYPE_REDUCE, tag, work))

            self.info("Sending keep-alive")
            self.__send_req('keep-alive')
            time.sleep(1)

    def __finished(self):
        with self.lock:
            dead = self.dead_man_walking

        return dead == True and             \
               self.num_map == 0 and        \
               self.num_reducer == 0 and    \
               len(self.map_queue) == 0 and \
               len(self.reduce_queue) == 0

    def __push_work(self, wstatus):
        if wstatus.type == TYPE_MAP:
            queue = self.map_queue
        elif wstatus.type == TYPE_REDUCE:
            queue = self.reduce_queue

        queue.append(wstatus)

    def __map_finished(self, msg, reduce_pushed):
        self.num_map -= 1

        with self.notifications_lock:
            self.notifications.append((msg, reduce_pushed))

    def __reduce_finished(self, msg):
        self.num_reducer -= 1

        with self.notifications_lock:
            self.notifications.append((msg, False))

    def pop_work(self):
        with self.lock:
            if len(self.map_queue) > 0:
                self.num_map += 1
                return self.map_queue.pop()
            elif len(self.reduce_queue) > 0:
                self.num_reducer += 1
                return self.reduce_queue.pop(0)
            else:
                return WorkerStatus(TYPE_DUMMY, 0, 1)

    def run(self):
        self.__deploy_workers()
        self.__main_loop()

    ##########################################################################
    # Main loop
    ##########################################################################

    def __deploy_workers(self):
        # Try to find the number of processing element trying to maximize it
        num_machines = min(self.n_machines,
                           max(self.conf['num-mapper'],
                               self.conf['num-reducer']))

        self.info("We will use %d slots" % (num_machines))

        for i in range(num_machines):
            comm = MPI.COMM_SELF.Spawn(sys.executable,
                                       args=["worker.py", self.fconf],
                                       maxprocs=1)
            self.communicators.add_channel(comm)

    def __main_loop(self):
        while not self.__finished():
            idx, comm = self.communicators.receive()

            msg = comm.recv()

            if msg.command == MSG_AVAILABLE:
                self.debug("Worker %d is available for a new task" % idx)
                self.__assign_work(idx, comm)

            elif msg.command == MSG_FINISHED_MAP:
                ret = self.on_map_finished(msg.result)

                if ret is not None:
                    self.__map_finished(msg, True)
                    self.__push_work(
                        WorkerStatus(TYPE_REDUCE, msg.tag, ret)
                    )
                else:
                    self.__map_finished(msg, False)

            elif msg.command == MSG_FINISHED_REDUCE:
                self.on_reduce_finished(msg.result)
                self.__reduce_finished(msg)

        self.info("Computation finished. Sending QUIT messages")

        for idx, comm in enumerate(self.communicators.channels):
            comm.send(Message(MSG_QUIT, None), dest=0)
            comm.Barrier()

        self.info("Waiting on the final barrier")

    def __assign_work(self, idx, comm):
        """
        Assign a job to a generic worker
        """

        wstatus = self.pop_work()

        trans = {TYPE_MAP:    MSG_COMPUTE_MAP,
                 TYPE_REDUCE: MSG_COMPUTE_REDUCE,
                 TYPE_DUMMY:  MSG_SLEEP}

        msg = trans[wstatus.type]

        self.debug("Assigning %s as role to worker %d" % \
                   (MSG_TO_STR[msg], idx))
        comm.send(Message(msg, wstatus.tag, wstatus.state), dest=0)

    ##########################################################################
    # Public function that can be overriden
    ##########################################################################

    def on_map_finished(self, result):
        return None

    def on_reduce_finished(self, result):
        self.info("Final result %s" % str(result))

    def input(self):
        raise Exception("Not implemented")






    # Useless shit

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


def start_mapreduce(mcls):
    if len(sys.argv) != 3:
        print("Usage: %s <nick> <conf-file>" % (sys.argv[0]))
        sys.exit(-1)

    if MPI.COMM_WORLD.Get_rank() == 0:
        mcls(sys.argv[1], sys.argv[2]).run()
