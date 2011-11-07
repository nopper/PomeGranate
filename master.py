import sys
import json
import time
import random
import urlparse

from mpi4py import MPI

from mux import Muxer
from status import MasterStatus
from httptransport import *
from threading import Lock, Thread, Semaphore, Timer, Event
from utils import Logger, load_module, count_machines
from message import *


class Master(Logger, HTTPClient):
    def __init__(self, nick, fconf):
        Logger.__init__(self, "Master")
        HTTPClient.__init__(self)

        self.fconf = fconf
        self.conf = json.load(open(fconf))
        self.status = MasterStatus()

        self.comm = MPI.COMM_WORLD
        self.n_machines = count_machines(self.conf["machine-file"])

        self.communicators = None
        self.units_to_kill = 0
        self.kill_lock = Lock()

        self.info("We have %d available slots" % (self.n_machines))

        self.nick = nick
        self.url = self.conf['master-url']

        self.num_map = 0

        self.lock = Lock()
        self.timer = None
        self.map_queue = []
        self.dead_man_walking = False

        self.ev_finished = Event()

        # Data structures for the reducers
        self.threshold_size = int(self.conf["threshold-size"])
        self.threshold_nfile = int(self.conf["threshold-nfile"])

        # Everything relative to the reducers should be locked against
        self.reduce_lock = Lock()

        # This holds the triples in the sense that for each reduce we have
        # a nested list which integers representing output of the mappers.
        # If we have two reducers we will have for example:
        # [
        #  [(0, 45), (1, 32), (3, 331)],
        #  [(5, 22), (6, 99)]
        # ]
        # Meaning:
        #  Reduce #1: -> output-reduce-000000-000000, 45 bytes
        #             -> output-reduce-000000-000001, 32 bytes
        #             -> output-reduce-000000-000003, 331 bytes
        #  Reduce #2: -> output-reduce-000001-000005, 22 bytes
        #             -> output-reduce-000001-000006, 99 bytes
        self.reducing_files = []

        self.reduce_started = []
        # TODO: Qui magari sarebbe meglio usare il modulo array? o forse lista
        #       dato che siamo dinamici e la taglia potrebbe aumentare.
        for _ in range(int(self.conf['num-reducer'])):
            self.reduce_started.append(False)
            self.reducing_files.append([])

        self.registered = False

        self.num_pending_request = Semaphore(self.n_machines)

        # Threading
        self.requester_thread = Thread(target=self.__requester_thread)
        self.main_thread = Thread(target=self.__main_loop)


    ##########################################################################
    # Utility functions to send request to the manager
    ##########################################################################

    def _on_change_degree(self, nick, data):
        self.info("Requested a parallelism degree change")
        # TODO: ignora se nella final phase
        if data < 0:
            with self.kill_lock:
                self.units_to_kill += abs(data)
        else:
            self.communicators.spawn_more(data)

            total = self.communicators.get_total()

            self.status.nproc = total
            self.__send_req('change-degree-ack', data=total)

    def _on_plz_die(self, nick, data):
        print "*" * 100
        self.info("Exit message received. Sending termination messages")
        self.communicators.send_all(Message(MSG_QUIT, 0, None))

        self.ev_finished.set()
        self.num_pending_request.release()

        self.close()

    def _on_try_later(self, nick, data):
        self.info("Waiting 1 second before unblocking requester.")

        if self.timer is None:
            self.timer = Timer(1, self._unblock_requester)
            self.timer.start()

    def _unblock_requester(self):
        self.num_pending_request.release()
        self.timer = None

    def _on_reques_error(self, nick, data):
        self.error("Error in request")

    def _on_connected(self):
        self.info("Succesfully connected to the server. Trying registration.")
        self.__send_req('registration', immediate=True)

    def _on_reduce_recovery(self, nick, data):
        print "O" * 80
        self.info("We need to recover something")
        self.reducing_files = data

    def _on_registration_ok(self, nick, data):
        with self.lock:
            if self.registered:
                self.error("Already registered")
            else:
                self.registered = True
                self.info("Succesfully registered")

    def _on_change_nick(self, nick, data):
        self.warning("Nick already used. Randomizing nick for your fun")
        self.nick = MPI.Get_processor_name() + str(random.randint(0, 100))
        self.warning("Your new nick is %s" % self.nick)
        self.__send_req('registration', immediate=True)

    def _on_end_of_stream(self, nick, data):
        with self.lock:
            self.dead_man_walking = True

    def _on_compute_map(self, nick, data):
        self.__push_work(WorkerStatus(TYPE_MAP, nick, data))

    def _on_keep_alive(self, nick, data):
        msg = {'timeprobe': data, 'status': self.status.serialize()}
        self.__send_req('keep-alive', data=msg, immediate=True)

    def __send_req(self, type, nick=None, data='', immediate=True):
        """
        Put the request in a buffer of requests. The buffer will be flushed
        in FIFO fashion whenever it is possible.
        """
        if nick is None:
            nick = self.nick

        pr = urlparse.urlparse(self.url)
        data = json.dumps({'type': type, 'nick': nick, 'data': data})
        self._add_request("POST %s HTTP/1.1\r\n" \
                          "Host: %s\r\n" \
                          "Connection: keep-alive\r\n"
                          "Content-Type: application/json\r\n" \
                          "Content-Length: %d\r\n\r\n%s" % \
                          (pr.path, pr.hostname, len(data), data), immediate)

    ##########################################################################
    # Threading
    ##########################################################################

    def __requester_thread(self):
        while not self.ev_finished.is_set():
            self.num_pending_request.acquire()
            print "ACQUIRING"
            self.__send_req('work-request')

        self.info("Requester thread exited correctly - AAAAAAAAAAAAAAAAAAAAAAA")

    def __finished(self):
        with self.lock:
            exit = self.dead_man_walking == True and \
                   self.num_map == 0 and             \
                   len(self.map_queue) == 0

        print "EXIT IS", exit

        if exit:
            return exit and self.__finished_reduce()

        return False

    def __finished_reduce(self):
        with self.reduce_lock:
            if not any(self.reduce_started):
                return True
        return False

    def __push_work(self, wstatus):
        with self.lock:
            self.map_queue.append(wstatus)

    def __map_finished(self, msg):
        with self.lock:
            self.num_map -= 1

        self.num_pending_request.release()

        # TODO: msg.result e' una lista di tuple di interi rappresentanti file di output
        #       (rid, fid, fsize)
        self.__send_req('map-ack', data=(msg.tag, msg.result))

        nfiles = 0
        filesize = 0

        for rid, fid, fsize in msg.result:
            nfiles += 1
            filesize += fsize
            self.reducing_files[rid].append((fid, fsize))

        self.status.increase(
            map_finished=1,
            map_ongoing=-1,
            map_file=nfiles,
            map_file_size=filesize
        )

    def __reduce_finished(self, msg, skip=False):
        if not skip:
            with self.reduce_lock:
                self.reduce_started[msg.tag] = False

        # This contain previously reduced files in input and the result file
        # data will be an array (<reduceidx>, (<outpu>, <inp1>, <inp2>, ...))
        self.__send_req('reduce-ack', data=(msg.tag, msg.result))

        self.status.increase(
            reduce_finished=1,
            reduce_ongoing=-1,
            reduce_file=1,
            reduce_file_size=msg.result[0][0]
        )

    def __pop_work(self):
        with self.lock:
            if len(self.map_queue) > 0:
                self.num_map += 1
                return self.map_queue.pop(0)

        return WorkerStatus(TYPE_DUMMY, 0, 1)

    def run(self):
        r = urlparse.urlparse(self.url)
        self.connect((r.hostname, r.port or 80))

        # Try to find the number of processing element trying to maximize it
        num_machines = min(self.n_machines,
                           max(self.conf['num-mapper'],
                               self.conf['num-reducer']))

        self.info("We will use %d slots" % (num_machines))
        self.communicators = Muxer(num_machines, ("worker.py", self.fconf))
        self.status.nproc = num_machines

        self.main_thread.start()
        self.requester_thread.start()

        HTTPClient.run(self)

    ##########################################################################
    # Main loop
    ##########################################################################

    def __got_killed(self, comm):
        to_kill = False

        with self.kill_lock:
            if self.units_to_kill > 0:
                self.units_to_kill -= 1
                to_kill = True

        if to_kill:
            comm.send(Message(MSG_QUIT, 0, None))
            self.communicators.remove(comm)

            total = self.communicators.get_total()

            self.status.nproc = total
            self.__send_req('change-degree-ack', data=total)

        return to_kill

    def __main_loop(self):
        while not self.__finished():
            idx, comm = self.communicators.receive()
            msg = comm.recv()

            if msg.command == MSG_AVAILABLE:
                self.debug("Worker %d is available for a new task" % idx)

                if not self.__got_killed(comm):
                    self.__assign_work(idx, comm)
                else:
                    self.info("Worker %d was killed as requested" % idx)

            elif msg.command == MSG_FINISHED_MAP:
                ret = self.on_map_finished(msg.result)
                self.__map_finished(msg)

            elif msg.command == MSG_FINISHED_REDUCE:
                self.on_reduce_finished(msg.result)
                self.__reduce_finished(msg)

        to_assign = int(self.conf["num-reducer"])

        self.info("Final phase. Start all the reducers")
        self.info("We have to execute %d reducer works" % to_assign)

        while to_assign > 0:
            idx, comm = self.communicators.receive()
            msg = comm.recv()

            if msg.command == MSG_AVAILABLE:
                self.debug("Worker %d is available for a new task" % idx)

                if not self.__got_killed(comm):
                    self.__assign_work(idx, comm, True)
                else:
                    self.info("Worker %d was killed as requested" % idx)

            elif msg.command == MSG_FINISHED_REDUCE:
                self.on_reduce_finished(msg.result)
                self.__reduce_finished(msg, True)
                to_assign -= 1
                print to_assign

        self.__merge_phase()

    def __merge_phase(self):
        # Let's reset all the status of the reducers
        with self.reduce_lock:
            for reduce_idx, reduce_list in enumerate(self.reducing_files):
                self.reducing_files[reduce_idx] = []
                self.reduce_started[reduce_idx] = False

        for _ in xrange(self.n_machines):
            print self.num_pending_request._Semaphore__value
            self.num_pending_request.release()

        self.info("Entering in the merge phase")

        while not self.ev_finished.is_set():
            idx, comm = self.communicators.receive()
            msg = comm.recv()

            if msg.command == MSG_AVAILABLE:
                self.debug("Worker %d is available for a new task" % idx)

                if not self.__got_killed(comm):
                    self.__assign_work(idx, comm, True)
                else:
                    self.info("Worker %d was killed as requested" % idx)

            elif msg.command == MSG_FINISHED_REDUCE:
                print "HERRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRE"
                self.on_reduce_finished(msg.result)
                self.__reduce_finished(msg)
                ###
                self.num_pending_request.release()

        self.__send_req('all-finished')

    def __assign_work(self, idx, comm, final_phase=False):
        """
        Assign a job to a generic worker
        """

        wstatus = self._check_threshold(final_phase)

        if wstatus is None:
            wstatus = self.__pop_work()
        else:
            with self.reduce_lock:
                self.reduce_started[wstatus.tag] = True

        trans = {TYPE_MAP:    MSG_COMPUTE_MAP,
                 TYPE_REDUCE: MSG_COMPUTE_REDUCE,
                 TYPE_DUMMY:  MSG_SLEEP}

        msg = trans[wstatus.type]

        if msg == MSG_COMPUTE_MAP:
            self.status.increase(map_ongoing=1)
        elif msg == MSG_COMPUTE_REDUCE:
            self.status.increase(reduce_ongoing=1)

        self.debug("Assigning %s as role to worker %d" % (MSG_TO_STR[msg], idx))
        comm.send(Message(msg, wstatus.tag, wstatus.state), dest=0)

    ##########################################################################
    # Public function that can be overriden
    ##########################################################################

    def _check_threshold(self, ignore_limits=False):
        """
        This function has to check the reducing_file_sizes and reducing_files
        and if a given threshold is met return a WorkerStatus representing a
        reduce operation.
        @return a WorkerStatus instane or None if a reduce is not required
        """

        # FIXME: In case this create load unbalancing substitute with an heap
        # or sort everything on modification.

        found = False
        reduce_idx = 0

        cum_size = 0
        num_files = 0

        with self.reduce_lock:
            for reduce_idx, reduce_list in enumerate(self.reducing_files):
                if self.reduce_started[reduce_idx]:
                    continue

                for fid, fsize in reduce_list:
                    cum_size += fsize
                    num_files += 1

                    if ignore_limits:
                        continue

                    if cum_size >= self.threshold_size or \
                       num_files >= self.threshold_nfile:
                        found = True
                        break

                if found:
                    break

                if ignore_limits:
                    found = True
                    break

                cum_size = 0
                num_files = 0

            if not found:
                return None

            files = self.reducing_files[reduce_idx]
            assigned = files[:num_files]

        files_id = map(lambda x: x[0], assigned)
        if not files_id:
            return None
        print "ASSIGN", reduce_idx, files_id
        return WorkerStatus(TYPE_REDUCE, reduce_idx, (reduce_idx, files_id))

    def on_map_finished(self, result):
        return None

    def on_reduce_finished(self, result):
        self.info("Final result %s" % str(result))

    def input(self):
        raise Exception("Not implemented")


def start_mapreduce(mcls):
    if len(sys.argv) != 3:
        print("Usage: %s <nick> <conf-file>" % (sys.argv[0]))
        sys.exit(-1)

    if MPI.COMM_WORLD.Get_rank() == 0:
        mcls(sys.argv[1], sys.argv[2]).run()
