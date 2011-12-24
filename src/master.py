"""
This module contains the implementation of the Master object which acts both as
a client with respect to the global server and as a server for the generic
workers.
"""

import sys
import json
import random
import os.path
import urlparse

from mpi4py import MPI

from mux import Muxer
from status import MasterStatus
from httptransport import HTTPClient
from threading import Lock, Thread, Semaphore, Timer, Event
from utils import Logger, count_machines
from message import *

class Master(Logger, HTTPClient):
    def __init__(self, nick, fconf):
        """
        Create a Master client/server
        @param nick a friendly name string for identification
        @param fconf path to the configuration file
        """

        Logger.__init__(self, "Master")
        HTTPClient.__init__(self)

        self.fconf = fconf
        self.conf = json.load(open(fconf))

        # This keep track of the statistics of the master. See status.py
        self.status = MasterStatus()

        # Set to true if the registration was succesful
        self.registered = False
        self.unique_id = -1

        # Marks the end of the stream. The server has no more maps to execute.
        # Set to true whenever a end-of-stream message is received
        self.end_of_stream = False

        self.comm = MPI.COMM_WORLD
        self.n_machines = count_machines(self.conf["machine-file"])

        # The mux object.
        self.communicators = None

        # The lock is used to synchronize the access to units_to_kill variable
        # which will be accessed by two different threads, namely the one
        # interacting with server and the one interacting with the workers
        self.kill_lock = Lock()
        self.units_to_kill = 0

        self.info("We have %d available slots" % (self.n_machines))

        self.nick = nick
        self.url = self.conf['master-url']
        self.sleep_inter = self.conf['sleep-interval']

        # Generic lock to synchronize the access to the instance variables of
        # the object itself. Its use should be minimized.
        self.lock = Lock()

        # Integer marking the number of maps which are currently being
        # executed. Incremented on assignment, decremented on finish.
        self.num_map = 0

        # Simple queue of WorkerStatus(TYPE_MAP, ..) objects. Filled whenever
        # the server returns us a compute-map message.
        self.map_queue = []

        # An event that whenever is set marks the end of the computation, set
        # upon reception of the plz-die message
        self.ev_finished = Event()

        # Maximum number of simultaneous files that the reduce may manage in
        # one row. Usually should be set to the MAX_FD of the system.
        self.threshold_nfile = int(self.conf["threshold-nfile"])

        # Simple lock that synchronize access to reduc* instance variables.
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

        # It will contain boolean values indicating the status of the reducers
        self.reduce_started = []

        for _ in xrange(int(self.conf['num-reducer'])):
            self.reduce_started.append(False)
            self.reducing_files.append([])

        # The timer will be used to unlock the semaphore that is used as
        # bounding mechanism for requesting new jobs to the server.
        self.timer = None
        self.num_pending_request = Semaphore(self.n_machines)

        # Here we start two simple thread one in charge of executing requests
        # and the other which is in charge of executing the main loop. There is
        # also another thread executing asyncore.loop that manages the http
        # communication with the server.
        self.requester_thread = Thread(target=self.__requester_thread)
        self.main_thread = Thread(target=self.__main_loop)

    ##########################################################################
    # Events handling
    ##########################################################################

    def _on_change_degree(self, nick, data):
        self.info("Requested a parallelism degree change")
        # TODO: Ignore in the final phase

        if data < 0:
            with self.kill_lock:
                self.units_to_kill += abs(data)
        else:
            self.communicators.spawn_more(data)

            total = self.communicators.get_total()

            self.status.nproc = total
            self.__send_req('change-degree-ack', data=total)

    def _on_plz_die(self, nick, data):
        self.info("Exit message received. Sending termination messages")
        self.communicators.send_all(Message(MSG_QUIT, 0, None))

        self.ev_finished.set()
        self.num_pending_request.release()

        self.close()

    def _on_try_later(self, nick, data):
        #self.info("Waiting 1 second before unblocking requester.")

        if self.timer is None:
            self.timer = Timer(1, self._unblock_requester)
            self.timer.start()

    def _unblock_requester(self):
        self.num_pending_request.release()
        self.timer = None

    def _on_request_error(self, nick, data):
        self.error("Error in request")

    def _on_connected(self):
        self.info("Succesfully connected to the server. Trying registration.")
        self.__send_req('registration', immediate=True)

    def _on_reduce_recovery(self, nick, data):
        self.info("We need to recover something %s" % str(data))
        self.reducing_files = data

    def _on_registration_ok(self, nick, data):
        with self.lock:
            if self.registered:
                self.error("Already registered")
            else:
                self.registered = True
                self.unique_id = data

                self.info("Succesfully registered with ID=%d" % data)
                self.__inner_start()

    def _on_change_nick(self, nick, data):
        self.warning("Nick already used. Randomizing nick for your fun")
        self.nick = MPI.Get_processor_name() + str(random.randint(0, 100))
        self.warning("Your new nick is %s" % self.nick)
        self.__send_req('registration', immediate=True)

    def _on_end_of_stream(self, nick, data):
        with self.lock:
            self.end_of_stream = True

    def _on_compute_map(self, nick, data):
        self.__push_work(WorkerStatus(TYPE_MAP, nick, data))

    def _on_keep_alive(self, nick, data):
        msg = {'timeprobe': data, 'status': self.status.serialize()}
        self.__send_req('keep-alive', data=msg, immediate=True)

    def __send_req(self, type, nick=None, data='', immediate=True):
        """
        Put the request in a buffer of requests. The buffer will be flushed
        in FIFO fashion whenever it is possible.

        @param type a string representing the type of the message
        @param nick set it to None to use the current nick
        @param data user data to send as payload to the message
        @param immediate True to insert the request in the first position of
                         the buffer in order to prioritize it
        """
        if nick is None:
            nick = self.nick

        url = urlparse.urlparse(self.url)
        data = json.dumps({'type': type, 'nick': nick, 'data': data})
        self._add_request("POST %s HTTP/1.1\r\n"               \
                          "Host: %s\r\n"                       \
                          "Connection: keep-alive\r\n"         \
                          "Content-Type: application/json\r\n" \
                          "Content-Length: %d\r\n\r\n%s" %     \
                          (url.path, url.hostname, len(data), data), immediate)

    def __requester_thread(self):
        while not self.ev_finished.is_set():
            self.num_pending_request.acquire()
            self.__send_req('work-request')

        self.info("Requester thread exited correctly")

    def __finished(self):
        """
        Check the status of the master
        @return True if the stream is finished, all the mapper returned and
                there is no reducer active
        """
        with self.lock:
            exit = self.end_of_stream == True and \
                   self.num_map == 0 and          \
                   len(self.map_queue) == 0

        if exit:
            return exit and self.__finished_reduce()

        return False

    def __finished_reduce(self):
        "@return True if there is no reducer started"

        with self.reduce_lock:
            if not any(self.reduce_started):
                return True
        return False

    def __pop_work(self):
        """
        Extract a job from the work_queue
        @return a WorkerStatus instance
        """
        with self.lock:
            if len(self.map_queue) > 0:
                self.num_map += 1
                return self.map_queue.pop(0)

        return WorkerStatus(TYPE_DUMMY, 0, self.sleep_inter)

    def __push_work(self, wstatus):
        """
        Insert a WorkerStatus object in the map_queue.
        @param wstatus a WorkerStatus instance
        """
        with self.lock:
            self.map_queue.append(wstatus)

    def __map_finished(self, msg):
        """
        Update the status of the master and send back ack to the server.

        The method is also responsible of pushing future reduce work in the
        reducing_files structure.

        @param the Message object returned by the generic worker
        """

        with self.lock:
            self.num_map -= 1

        self.num_pending_request.release()

        # Note: msg.result is a list of tuples representing output files
        #       in the form: (rid, fid, fsize)
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
            map_file_size=filesize,
            bandwidth=msg.info[0],
            time=msg.info[1],
        )

    def __reduce_finished(self, msg, skip=False):
        """
        Update the status of the master and send back ack to the server.
        @param the Message object returned by the generic worker
        """

        if not skip:
            with self.reduce_lock:
                self.reduce_started[msg.tag] = False
                self.reducing_files[msg.tag].append(msg.result[0])

        # This contain previously reduced files in input and the result file
        # data will be an array (<reduceidx>, (<outpu>, <inp1>, <inp2>, ...))
        self.__send_req('reduce-ack', data=(msg.tag, msg.result))

        self.status.increase(
            reduce_finished=1,
            reduce_ongoing=-1,
            reduce_file=1,
            reduce_file_size=msg.result[0][1],
            bandwidth=msg.info[0],
            time=msg.info[1],
        )

    ##########################################################################
    # Main loop
    ##########################################################################

    def __got_killed(self, comm):
        """
        This method is called in order to accomodate parallelism degree change
        during the computation. It returns a boolean indicating if the
        communicator has been killed or not.

        @return True if comm was killed
        """
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

            # Note we will send this message at every kill. This can be summed
            # up in order to avoid useless messages traveling the network.
            self.__send_req('change-degree-ack', data=total)

        return to_kill

    def __main_loop(self):
        """
        This is the main loop of the application. It is organized in three
        loops.

         1. The first will continue until the stream is finished and all the
            maps and reducers assigned will eventually return.
         2. The second loop will start by assigning at most num-reducer reducer
            jobs to the generic workers. This is needed to reduce all the
            partial files that may be produced by the first loop.
         3. Eventually a third merge cycle is started until ev_finished is set.
            This is needed in order to have a global merging scheme that is
            conducted with the assistance of the global server which is in
            charge of orchestrating this phase.
        """

        while not self.__finished():
            idx, comm = self.communicators.receive()
            # Here we wait until all the map are assigned and also all the
            # assigned reduce are finished.
            msg = comm.recv()

            if msg.command == MSG_AVAILABLE:
                if not self.__got_killed(comm):
                    self.__assign_work(idx, comm)
                else:
                    self.info("Worker %d was killed as requested" % idx)

            elif msg.command == MSG_FINISHED_MAP:
                self.on_map_finished(msg.result)
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
                if not self.__got_killed(comm):
                    # Here we need to check about the type of assignment made.
                    # If we don't have any more reduce to assign it convenient
                    # to get out of the cycle
                    if self.__assign_work(idx, comm, True) == TYPE_DUMMY:
                        to_assign -= 1
                else:
                    self.info("Worker %d was killed as requested" % idx)

            elif msg.command == MSG_FINISHED_REDUCE:
                self.on_reduce_finished(msg.result)
                self.__reduce_finished(msg, True)
                to_assign -= 1

        if not self.ev_finished.is_set():
            self.__merge_phase()
        else:
            self.info("Merge was not necessary")

    def __merge_phase(self):
        # Let's reset all the status of the reducers
        with self.reduce_lock:
            for reduce_idx, reduce_list in enumerate(self.reducing_files):
                self.reducing_files[reduce_idx] = []
                self.reduce_started[reduce_idx] = False

        for _ in xrange(self.n_machines):
            self.num_pending_request.release()

        self.info("Entering in the merge phase")

        while not self.ev_finished.is_set():
            idx, comm = self.communicators.receive()
            msg = comm.recv()

            if msg.command == MSG_AVAILABLE:
                if not self.__got_killed(comm):
                    self.__assign_work(idx, comm, True)
                else:
                    self.info("Worker %d was killed as requested" % idx)

            elif msg.command == MSG_FINISHED_REDUCE:
                self.on_reduce_finished(msg.result)
                self.__reduce_finished(msg)
                ###
                self.num_pending_request.release()

        self.__send_req('all-finished')

    def __assign_work(self, idx, comm, final_phase=False):
        """
        Assign a job to a generic worker

        @param idx the index as returned by the communicators struct
        @param comm the MPI Intercommunicator
        @param final_phase True if we have to assign to the reducers all the
                           files available if we are approaching the last phase
        @return an integer indicating the type of work assigned
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

        return wstatus.type

    def _check_threshold(self, ignore_limits=False):
        """
        This function has to check the reducing_file_sizes and reducing_files
        and if a given threshold is met return a WorkerStatus representing a
        reduce operation.

        @param ignore_limits True if we have to skip threshold-nfile limit
                             check
        @return a WorkerStatus instance or None if a reduce is not required
        """

        overflow = False
        valid_found = False
        reduce_idx = 0

        num_files = 0

        with self.reduce_lock:
            for reduce_idx, reduce_list in enumerate(self.reducing_files):

                # Skip already started reducers
                if self.reduce_started[reduce_idx]:
                    continue

                for fid, _ in reduce_list:
                    num_files += 1

                    if ignore_limits:
                        continue

                    if num_files >= self.threshold_nfile:
                        overflow = True
                        break

                if num_files > 1:
                    valid_found = True
                    break
                else:
                    num_files = 0
                    overflow = False
                    continue

        if valid_found:
            # The last valid reduce_idx will be our target
            files = self.reducing_files[reduce_idx]
            self.reducing_files[reduce_idx] = files[num_files:]
            assigned = files[:num_files]

            files_id = map(lambda x: x[0], assigned)

            self.info("Files to reduce %s [overflow check: %s]" % \
                     (str(files_id), str(overflow)))

            return WorkerStatus(TYPE_REDUCE, reduce_idx, (reduce_idx, files_id))
        else:
            return None

    ##########################################################################
    # Public functions
    ##########################################################################

    def run(self):
        """
        Start the master and connect it to the server indicated in the
        configuration file.
        """
        url = urlparse.urlparse(self.url)
        self.connect((url.hostname, url.port or 80))

        HTTPClient.run(self)

    def __inner_start(self):
        self.info("Starting requester and main thread")

        # Try to find the number of processing element trying to maximize it
        num_machines = min(self.n_machines,
                           max(self.conf['num-mapper'],
                               self.conf['num-reducer']))

        self.info("We will use %d slots" % (num_machines))

        filename = 'worker' + __file__[__file__.rindex('.'):]
        filename = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), filename
        )

        self.debug("Using %s as spawner" % filename)

        self.communicators = Muxer(self.unique_id, num_machines, (filename, self.fconf))
        self.status.nproc = num_machines

        self.main_thread.start()
        self.requester_thread.start()

    def on_map_finished(self, result):
        "You are free to override this"
        pass

    def on_reduce_finished(self, result):
        "You are free to override this"
        self.info("Final result %s" % str(result))

def start_mapreduce(mcls):
    if len(sys.argv) != 3:
        print("Usage: %s <nick> <conf-file>" % (sys.argv[0]))
        sys.exit(-1)

    if MPI.COMM_WORLD.Get_rank() == 0:
        mcls(sys.argv[1], sys.argv[2]).run()
