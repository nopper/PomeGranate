import os
import sys
import json
import time
import logging

from status import ApplicationStatus
from utils import Logger, load_module, get_file_name
from message import WorkerStatus, TYPE_MAP, TYPE_REDUCE

from jinja2 import Environment, FileSystemLoader, Template
from threading import Thread, Lock, Event
from collections import defaultdict
from httpserver import RequestHandler, Server

PING_DONE = 1
PING_EXECUTE = 2

CHANGE_REQUESTED    = 1
CHANGE_SENT         = 2
CHANGE_ACKNOWLEDGED = 3

class Handler(RequestHandler):
    def __init__(self, conn, addr, server):
        RequestHandler.__init__(self, conn, addr, server)

        self.nick = None
        self.rtt = 0
        self.time_probe = 0
        self.waiting_ping_response = False

        self.par_degree = 0
        self.par_degree_changed = CHANGE_ACKNOWLEDGED

        self.eos_sent = False

    def handle_close(self):
        RequestHandler.handle_close(self)

        if self.nick is not None:
            self.server.on_group_died(self.nick, False)

    def handle_error(self):
        RequestHandler.handle_error(self)

        if self.nick is not None:
            self.server.on_group_died(self.nick, True)

    def do_GET(self):
        self.server.status.now_time = int(time.time())

        if self.path == "/":
            # This should be responsible for user view
            tmpl = self.server.env.get_template('index.html')
            data = tmpl.render(status=self.server.status)

            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.send_header('Content-Length', len(data))
            self.end_headers()
            self.wfile.write(data)
        elif self.path == "/status":
            res = self.server.status.serialize()
            #print res
            data = json.dumps(res)

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Content-Length', len(data))
            self.end_headers()
            self.wfile.write(data)
        elif self.path == "/favicon.ico":
            # Don't worry it's not a shellcode. Just a little pomegranate.
            data = "\x1f\x8b\x08\x00\x1a\x5c\xbd\x4e\x02\xff\x7d\x93\x5d\x48" \
                   "\x53\x61\x18\xc7\x5f\x51\x13\x22\xc8\xab\xa0\x8b\xa8\x08" \
                   "\x75\x4b\x41\x48\x41\xad\x0b\x25\x49\xba\x30\x4c\x10\xba" \
                   "\x50\x48\x32\x69\x51\x90\x98\x49\x20\x51\x57\xdd\x94\x17" \
                   "\x85\x46\x98\x1f\xab\x99\x9b\x8a\x9f\xac\xa6\x73\x56\xba" \
                   "\x48\x51\x2c\x29\x8d\x44\xcf\xdc\x3a\x9e\x8f\xb9\x39\xb5" \
                   "\xa5\xce\xa3\xdb\xbf\xf7\x4c\x56\x53\x97\xcf\xe1\x7f\x2e" \
                   "\xde\xf3\xfe\xce\xf3\x7f\x9e\xf7\x79\x09\x09\xa3\x4f\x74" \
                   "\x34\xa1\xef\x63\x44\x15\x41\xc8\x21\x42\x88\x82\x8a\x2e" \
                   "\xd1\x95\xad\x75\x7f\x44\x90\x5d\x01\xe0\xaf\x36\x36\x7c" \
                   "\xe1\x63\x63\x6b\xa7\x2b\x2a\xec\x35\x19\x19\x36\x4b\x7c" \
                   "\x3c\xe3\x49\x88\x67\xd6\xd2\x33\xac\x96\xf2\x72\x51\x3d" \
                   "\x34\xb4\x92\x29\x49\xbe\xc8\x60\x26\x20\x51\xdc\x38\x5c" \
                   "\x56\x66\xd7\xc8\x4c\x6c\x2c\x83\x2d\xcd\xa0\x5a\x6d\xc5" \
                   "\xb3\x66\x01\x85\xd7\x6d\x48\x4a\x66\x24\x95\x8a\xef\xb6" \
                   "\xd9\xd6\x4f\x04\xb3\x1c\x27\x1d\xc9\xcb\x9b\x1b\xa6\x8c" \
                   "\xf7\x1f\xcb\x40\xa9\x98\xc6\x73\xed\x1c\x34\x06\x11\x66" \
                   "\xce\x8d\xee\xf1\x25\x5c\xba\xc2\x22\x3b\xfb\xe7\x37\x8b" \
                   "\x65\x3d\x56\x66\xd7\xd7\x7d\xfb\x54\x2a\xa1\x63\x27\x1b" \
                   "\x23\xf3\xca\x69\x68\xfb\x04\x74\x8c\x2c\xa0\x5a\xcb\xa3" \
                   "\xe5\xe3\x02\x7a\xbe\x2f\xe1\xf2\x0d\xce\x9b\x9f\xcf\xbd" \
                   "\x5f\x5d\xf5\xee\x37\x99\x7e\x5f\xd8\xee\x79\x3b\xdf\xdc" \
                   "\x2f\xa0\x6b\xc4\x89\x2a\x9d\x88\xb6\x4f\x4e\xbc\x36\x39" \
                   "\xf0\xb8\x96\x43\x7a\xa6\x55\x6a\x6e\x5e\xbe\x4a\x73\x77" \
                   "\xed\xcc\x1d\xe0\x4f\x52\xbe\xa6\x95\xc3\x8b\xb6\x2d\x0f" \
                   "\x55\x3a\x3b\x5a\x07\xe7\xf1\xf6\xab\x0b\x77\x1e\x8a\xde" \
                   "\xdc\x8b\xec\x48\x6a\xea\xac\xb8\x93\x0d\xe6\x5f\xe9\x05" \
                   "\x68\xdf\x39\xd0\x44\x55\xa7\x77\x40\x37\xe0\x44\x27\xf5" \
                   "\x53\xdf\xe3\x42\x42\x02\xe3\x09\xe5\x3d\x98\x6f\x1b\x14" \
                   "\xa1\xff\xe2\x42\x65\x03\xad\x63\x74\x01\x8d\xfd\x4e\xa8" \
                   "\xf5\x22\x5a\x87\x97\xa0\x50\x32\x48\x49\x99\x9d\xdf\x8b" \
                   "\x7f\x49\xf3\x37\xf5\xcf\x43\xd3\xe7\x40\x6d\xf7\xbc\xdf" \
                   "\x47\xc7\xb0\x13\x0d\xc6\x45\x7f\xfe\xe2\x62\x5e\xbf\x57" \
                   "\xfd\x75\xed\x3c\xea\xbb\x04\x74\x8e\xba\x50\xdd\x42\xeb" \
                   "\x37\x3b\x60\x98\x58\xc4\xdd\x47\x76\x7a\x8e\xec\xb8\xc1" \
                   "\xe0\xce\xfb\x5f\x0d\x32\xaf\x33\x09\xfe\xbe\x3f\xd1\x88" \
                   "\x68\x31\x3b\xd1\x48\xfb\x5f\x59\xcf\xe3\x6c\x96\x55\x6a" \
                   "\x50\x2f\xdd\xa2\xe7\x1f\x55\x54\xc4\xbf\x89\x89\x99\xd9" \
                   "\xe5\x41\x9e\x9f\xa6\x5e\xc1\xcf\x3d\x6d\x14\xfc\xbd\x33" \
                   "\xfe\x58\x46\xf1\x6d\x1e\xb9\xb9\xec\xe8\xf2\xf2\xe6\x41" \
                   "\x79\x86\x6c\x36\xe9\x78\x4e\x0e\xfb\x39\xe4\xfc\xe9\x38" \
                   "\xbf\x7f\x33\xef\x46\xef\xd4\x2f\x14\xde\x9c\xc3\xb9\x2c" \
                   "\x9b\x65\x72\xd2\x73\x2a\x78\x86\x59\x56\x3a\x4a\x67\xa1" \
                   "\x5d\xa9\x64\xa4\x00\xaf\x88\x9b\x41\x43\x3b\x0b\xb5\xc1" \
                   "\x81\x92\x7b\x1c\x52\xcf\x58\xbc\x05\x05\xec\xd0\xd4\x94" \
                   "\x27\x29\xd4\x1d\xa2\xb5\x44\x0e\x0c\xae\x9c\x2f\x2d\x15" \
                   "\xb5\x69\x69\xb3\x76\xf9\x1f\x71\xf4\x0e\x25\x27\x33\xee" \
                   "\x6b\x2a\xbe\xcf\xd0\xe3\x2e\xa0\x33\x7b\x20\x14\xbb\x53" \
                   "\x13\x13\x9e\xa4\xc4\x44\xcb\x0a\xf5\xb3\x69\x34\xba\x4a" \
                   "\x7c\x3e\x5f\xc8\x7d\x9e\xa3\x84\x58\xa3\x08\xf9\x10\x4e" \
                   "\xc8\x83\xb0\x2d\x85\x8a\xc0\x37\x79\x9f\xbc\x5f\xe6\x7c" \
                   "\xf7\x09\xf9\x03\xdb\xea\xc0\x08\x7e\x04\x00\x00"

            self.send_response(200)
            self.send_header('Content-Encoding', 'gzip')
            self.send_header('Content-type', 'image/x-icon')
            self.send_header('Content-Length', len(data))
            self.end_headers()
            self.wfile.write(data)
        else:
            data = "<pre>Nothing to see here.</pre>"
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.send_header('Content-Length', len(data))
            self.end_headers()
            self.wfile.write(data)

    def do_POST(self):
        clen = self.headers.getheader('content-length')
        request = json.loads(self.rfile.read(int(clen)))

        type = request['type']
        nick = request['nick']
        data = request['data']

        meth = None

        try:
            meth = getattr(self, '_on_' + type.replace('-', '_'))
        except:
            print "No method defined for message type %s" % type

        if meth is not None:
            meth.__call__(self.server, type, nick, data)

    def _on_change_degree(self, server, type, nick, data):
        if nick not in server.masters:
            server.error("No group named %s" % nick)
            return

        server.info("Par-degree change requested: %s -> %d" % (nick, data))

        handler = server.masters[nick]

        handler.par_degree = data
        handler.par_degree_changed = CHANGE_REQUESTED

        self.close_connection = 1

    def _on_change_degree_ack(self, server, type, nick, data):
        if nick not in server.masters:
            return

        self.par_degree_changed = CHANGE_ACKNOWLEDGED
        self.par_degree = data

    def _on_registration(self, server, type, nick, data):
        if nick not in server.masters:
            self.nick = nick
            server.masters[nick] = self

            self.__send_data('registration-ok')
            server.info("Group %s succesfully registered" % nick)

            if nick in server.dead_reduce_dict:
                lst = server.dead_reduce_dict[nick]
                server.reduce_dict[nick] = lst
                del server.dead_reduce_dict[nick]

                self.__send_data('reduce-recovery', data=lst)
                server.info("Sending recovery message to %s" % nick)
            else:
                # Just prepare an empty data structure

                lst = []
                for _ in range(server.num_reducer):
                    lst.append([])

                server.reduce_dict[nick] = lst

            server.status.update_master_status(self.nick, {'status': 'online'})
        else:
            self.__send_data('change-nick')
            server.warning("Collision. Group %s already registered." % nick)

    def _on_work_request(self, server, type, nick, data):
        if not nick in server.masters:
            self.__send_data('registration-needed')
            return

        if server.merge_phase:
            lst = server.reduce_dict[nick]
            if lst is not None:
                server.status.reduce_assigned += 1
                server.info("Assigning merg work %s" % str(lst))
                self.__send_data('reduce-recovery', data=lst)
            else:
                server.info("Sending termination message")
                self.__send_data('plz-die')

        # If we have a work in the queue just assing it
        wstatus = server.work_queue.pop()

        if wstatus is not None:
            server.pending_works[nick].append(wstatus)
            server.status.map_assigned += 1

            server.info("Assigning work %s (tag: %s) to %s" % \
                        (str(wstatus.state), str(wstatus.tag), nick))

            self.__send_data('compute-map', wstatus.tag, wstatus.state)

        else:
            if len(server.pending_works) == 0:
                completed = self._reduce_completed()
                need_recovery = self._need_recovery()

                if completed and not need_recovery:
                    self.assign_merge()

                    lst = server.reduce_dict[nick]
                    if lst is not None:
                        server.status.reduce_assigned += 1
                        self.__send_data('reduce-recovery', data=lst)
                    else:
                        self.__send_data('plz-die')

                    return

                elif completed and need_recovery:
                    # This will converge slowly but better than nothing
                    zombie, lst = server.dead_reduce_dict.popitem()
                    server.info("Reassinging jobs from %s to %s -> %s" % \
                                (zombie, nick, str(lst)))

                    server.reduce_dict[nick] = lst
                    server.status.reduce_assigned += 1
                    self.__send_data('reduce-recovery', data=lst)

                else:
                    # If we have not any work nor pending works to acknowledge the
                    # computation is over so it's the perfect moment for a funeral
                    server.info("Stream completed. It is time for the reducers")
                    server.status.phase = server.status.PHASE_REDUCE

                    if not self.eos_sent:
                        self.__send_data('end-of-stream')
                        self.eos_sent = True

            # Otherwise just tell to the worker to retry in few moments
            #server.dbg("Pending: %s" % str(server.pending_works))
            #server.dbg("Reducing: %s" % str(server.reduce_dict))
            #server.dbg("Dead list: %s" % str(server.dead_reduce_dict))
            self.__send_data('try-later')

    def assign_merge(self):
        server = self.server

        if server.merge_phase:
            return

        server.merge_phase = True
        server.status.phase = server.status.PHASE_MERGE

        reduce_work = []
        for i in range(server.num_reducer):
            reduce_work.append([])

        # Extract all the works from the reduce dict
        for nick, reducers in server.reduce_dict.items():
            for reduce_idx, reduce_lst in enumerate(reducers):
                reduce_work[reduce_idx].extend(reduce_lst)

        # Recreate it
        pos = 0
        nicks = server.masters.keys()
        maxnick = len(nicks)
        server.reduce_dict = defaultdict(list)

        for reduce_idx, files in enumerate(reduce_work):
            jobs = [(), ] * server.num_reducer
            jobs[reduce_idx] = files

            if len(files) > 1:
                server.reduce_dict[nicks[pos]] = jobs
            else:
                server.reduce_dict[nicks[pos]] = None

            pos = (pos + 1) % maxnick

        server.info("After the merge we have %s" % str(server.reduce_dict))

    def _on_all_finished(self, server, type, nick, data):
        # TODO: remove me. Superflous
        self.__send_data('plz-die')

    def _reduce_completed(self):
        """
        Utility function that checks if the assigned reduce have been
        succesfully completed. No check is made on the dead structure.
        """

        for nick, reducers in self.server.reduce_dict.items():
            if not reducers:
                continue

            for reduce_lst in reducers:
                if len(reduce_lst) > 1:
                    return False

        return True

    def _need_recovery(self):
        """
        Return a boolean indicating if there are reduce works to recovery.
        This is of course implemented with a check in the dead structure.
        """

        if self.server.dead_reduce_dict:
            return True
        else:
            return False

    def __send_data(self, type, nick='', data='', code=200):
        payload = json.dumps({
            "type": type,
            "nick": nick,
            "data": data
        })

        self.send_response(code)
        self.send_header('Content-Length', len(payload))
        self.end_headers()
        self.wfile.write(payload)

    def _on_map_ack(self, server, type, nick, data):
        if nick not in server.masters:
            self.__send_data('registration-needed')
            return

        tag  = data[0]
        files = data[1]

        jobs = server.pending_works[nick]
        found = False

        for pos, wstatus in enumerate(jobs):
            if tag == wstatus.tag:
                found = True
                break

        if found:
            del server.pending_works[nick][pos]

            if not server.pending_works[nick]:
                del server.pending_works[nick]

            nfile = 0
            size = 0

            for rid, fid, fsize in files:
                size += fsize
                nfile += 1
                server.reduce_dict[nick][rid].append((fid, fsize))

            server.status.map_completed += 1
            server.status.map_file += nfile
            server.status.map_file_size += size
            server.status.add_graph_point()

            server.info("Map acknowledged correctly for group %s" % (nick))
        else:
            server.error("No work found with tag %d %s for group %s" % \
                            (tag, str(jobs), nick))

            self.__send_data('map-ack-fail', nick, data)

    def _on_reduce_ack(self, server, type, nick, data):
        if not nick in server.masters:
            self.__send_data('registration-needed')
            return

        reduce_idx = data[0]
        to_add     = data[1][0] # NB: This is a tuple (fid, fsize)
        to_delete  = data[1][1:] # NB: This is instead a sequence of [fid, ..]

        jobs = server.reduce_dict[nick][reduce_idx]

        opos = 0
        dpos = 0

        # TODO: maybe it should be optimized
        while opos < len(jobs):
            ofid, ofsize = jobs[opos]

            dpos = 0
            while dpos < len(to_delete):
                dfid = to_delete[dpos]

                if ofid == dfid:
                    fname = get_file_name(server.path, reduce_idx, dfid)
                    server.info("Removing map output file %s" % fname)
                    os.unlink(fname)

                    del to_delete[dpos]
                    del jobs[opos]

                    opos -= 1
                    break

                dpos += 1
            opos += 1

        jobs.append(tuple(to_add))

        server.status.reduce_assigned += 1
        server.status.reduce_completed += 1
        server.status.reduce_file += 1
        server.status.reduce_file_size += to_add[1]
        server.status.add_graph_point()

        if len(to_delete) > 0:
            server.error("Failed to remove reduce files %s" % str(to_delete))
            self.__send_data('reduce-ack-fail', nick, data)

    def _on_keep_alive(self, server, type, nick, data):
        if not nick in server.masters:
            self.__send_data('registration-needed', code=400)
            return

        if self.waiting_ping_response:
            self.rtt = data['timeprobe'] - self.time_probe
            self.time_probe = 0
            self.waiting_ping_response = False

            status = data['status']
            status['rtt'] = self.rtt
            self.server.status.update_master_status(self.nick, status)

            self.server.timestamps[nick] = (PING_DONE, self.rtt)
            #server.info("Round-Trip-Time for %s is %.10f" % (self.nick, self.rtt))

    def writable(self):
        if self.nick is None:
            return RequestHandler.writable(self)

        nick = self.nick

        if self.par_degree_changed == CHANGE_REQUESTED:
            self.par_degree_changed = CHANGE_SENT

            data = json.dumps({
                'type': 'change-degree',
                'nick': self.nick,
                'data': self.par_degree,
            })

            self.push("HTTP/1.1 200 OK\r\n"
                "Content-type: application/json\r\n"
                "Connection: keep-alive\r\n"
                "Content-Length: %d\r\n\r\n%s" % \
                (len(data), data))

        with self.server.lock:
            if not self.waiting_ping_response and \
                nick in self.server.timestamps and \
                self.server.timestamps[nick][0] == PING_EXECUTE:
                self.time_probe = time.time()

                data = json.dumps({
                    'type': 'keep-alive',
                    'nick': self.nick,
                    'data': self.time_probe,
                })

                #self.server.info("Requesting RTT for %s" % self.nick)

                self.push("HTTP/1.1 200 OK\r\n"
                        "Content-type: application/json\r\n"
                        "Connection: keep-alive\r\n"
                        "Content-Length: %d\r\n\r\n%s" % \
                        (len(data), data))

                self.waiting_ping_response = True

        return RequestHandler.writable(self)

class WorkQueue(object):
    def __init__(self, gen):
        self.generator = gen
        self.dead_queue = []
        self.last_tag = 0

    def push(self, item):
        self.dead_queue.append(item)

    def next(self):
        self.last_tag += 1

        try:
            value = self.generator.next()
        except StopIteration:
            if self.dead_queue:
                value = self.dead_queue.pop(0)
            else:
                value = None

        if value is not None:
            return WorkerStatus(TYPE_MAP, self.last_tag, value)

        return None

    def pop(self):
        return self.next()

class PushHandler(logging.Handler):
    def __init__(self, callback):
        logging.Handler.__init__(self)
        self.callback = callback

    def emit(self, record):
        self.callback(record)

class MasterServer(Server, Logger):
    def __init__(self, fconf, handler):
        Logger.__init__(self, "Manager")

        conf = json.load(open(fconf))

        self.env = Environment(loader=FileSystemLoader('templates'))
        self.status = ApplicationStatus()
        self.logger.addHandler(PushHandler(self.status.push_log))

        # This is a dictionary structure in the form
        # reduce_dict["group-name"] = [
        #   [ file list by unique integers, size in byte
        #   ] => Reduce-0
        #   [
        #   ] => Reduce-1
        # ]
        self.reduce_dict = defaultdict(list)
        self.dead_reduce_dict = defaultdict(list)

        self.num_reducer = int(conf["num-reducer"])
        self.path = conf["server-path"]

        # Load the input module and assing the generator to a member
        module = load_module(conf["input-module"])
        cls = getattr(module, "Input", None)

        self.generator = cls(fconf).input()
        self.work_queue = WorkQueue(self.generator)

        self.lock = Lock()
        self.update_rtt = False

        self.masters = {}
        self.pending_works = defaultdict(list) # nick => [work, ...]
        self.merge_phase = False

        # Ping thread
        self.finished = Event()
        self.timestamps = {} # nick => (send_ts:int, ts:float)
        self.hb_thread = Thread(target=self.hearthbeat)

        self.addrinfo = (conf['master-host'], conf['master-port'])
        Server.__init__(self, self.addrinfo[0], self.addrinfo[1], handler)

    def run(self):
        self.info("Server started on http://%s:%d ^C to stop it" % self.addrinfo)
        self.hb_thread.start()
        Server.run(self)

    def on_group_died(self, nick, is_error):
        # Possiamo magari ristartare tramite uno script bash su un altro server
        # remoto.

        self.status.update_master_status(nick, {'status': 'dead'})
        self.status.faults += 1

        # Remove any pending map activity
        lst = self.pending_works[nick]
        del self.pending_works[nick]

        for wstatus in lst:
            self.status.map_faulted += 1
            self.work_queue.push(wstatus.state)

        # Remove any pending reduce activity
        lst = self.reduce_dict[nick]

        if lst: # This might be None
            self.dead_reduce_dict[nick] = lst
            for reducer_lst in lst:
                if reducer_lst:
                    self.status.reduce_faulted += 1

        del self.reduce_dict[nick]
        del self.masters[nick]

    def hearthbeat(self):
        while not self.finished.is_set():
            ts = time.time()

            with self.lock:
                for nick in self.masters:
                    self.timestamps[nick] = (PING_EXECUTE, 0)

            time.sleep(5)

            with self.lock:
                for nick in self.masters:
                    type, ts = self.timestamps.get(nick, (None, None))

                    if type is not None:
                        print "Nick", nick, self.timestamps[nick]

    def stop(self):
        self.finished.set()

def main(fconf):
    try:
        server = MasterServer(fconf, Handler)
        server.run()
    except KeyboardInterrupt:
        print('^C received, shutting down server..')
        server.stop()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: %s <config-file>" % (sys.argv[0])
        sys.exit(-1)

    main(sys.argv[1])
