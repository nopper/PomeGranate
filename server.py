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
            data = json.dumps(self.server.status.__dict__, sort_keys=True)

            print data
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Content-Length', len(data))
            self.end_headers()
            self.wfile.write(data)
        elif self.path == "/favicon.ico":
            # Don't worry it's not a shellcode. Just a little lego block
            data = "\x1f\x8b\x08\x00\x0e\x2d\xb4\x4e\x02\xff\xed\x94\xbd\x11" \
                   "\xc2\x30\x0c\x46\x25\xdb\x72\x62\xc9\x05\x15\x35\x25\x83" \
                   "\xb0\x02\x2b\x70\x6c\x40\x4b\xc9\x0a\xac\xc0\x0a\xac\xc0" \
                   "\x0a\xac\xc0\x0a\xf0\xc9\x21\xc7\x4f\x4b\xc7\xe5\xe5\xde" \
                   "\xc9\x5f\x62\xcb\x67\x17\x21\x62\x3c\xb3\x19\x35\xb6\x42" \
                   "\x34\x47\x5d\x42\x7f\xb5\x80\x4c\xfd\xf0\x91\xe9\xc5\xfb" \
                   "\x98\x6e\xf0\x0c\x0f\x70\x0d\xaf\xf0\x04\x77\x70\x05\xef" \
                   "\xf0\x02\x8f\x70\x43\x13\x13\x13\x7f\x42\xef\xd4\x11\x4b" \
                   "\x1f\x64\x2b\x1f\x20\x4b\x30\xd3\x52\x24\x5b\x16\xcf\xa1" \
                   "\x4b\x7d\x64\xb6\x2e\xa5\xe0\x59\x7d\xcc\x22\x2c\xd5\xca" \
                   "\xb0\x9e\x83\x78\xd1\x3c\x66\x8f\xec\xe5\x99\xb9\x3c\x0b" \
                   "\x32\x8b\xaa\xf8\x1c\x95\x96\x35\xd6\x9a\xd1\xa2\x4b\x51" \
                   "\xdb\x7e\xd9\x52\x65\xc9\x39\xf6\xad\x1f\xe6\x86\x28\x58" \
                   "\xcb\x86\xfd\x6a\x6b\xa4\xec\xfd\x25\x8c\xf9\xd5\xbf\x7e" \
                   "\x9d\x87\xf6\xbf\xdc\x1d\xfe\xa1\x0f\xe5\x7d\xa4\x25\x7e" \
                   "\x05\x00\x00"

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

            if nick in server.reduce_dict:
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

            server.status.update_master_status(self.nick)


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
                if self._reduce_finished():
                    print "ASSIGNING MERGE"
                    self.assign_merge()

                    lst = server.reduce_dict[nick]
                    if lst is not None:
                        server.status.reduce_assigned += 1
                        self.__send_data('reduce-recovery', data=lst)
                    else:
                        self.__send_data('plz-die')

                    return

                else:
                    # If we have not any work nor pending works to acknowledge the
                    # computation is over so it's the perfect moment for a funeral
                    server.info("Stream completed. It is time for the reducers")
                    server.status.phase = server.status.PHASE_REDUCE

                    if not self.eos_sent:
                        self.__send_data('end-of-stream')
                        self.eos_sent = True

            # Otherwise just tell to the worker to retry in few moments
            server.info("You have to wait my little friend")
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

    def _reduce_finished(self):
        server = self.server

        if not server.dead_reduce_dict:
            for nick, reducers in server.reduce_dict.items():
                for reduce_lst in reducers:
                    if len(reduce_lst) > 1:
                        return False
            return True
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
        found = False

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

        if len(to_delete) > 0:
            server.error("Failed to remove reduce files %s" % str(to_delete))
            self.__send_data('reduce-ack-fail', nick, data)

        print server.reduce_dict

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
            server.info("Round-Trip-Time for %s is %.10f" % (self.nick, self.rtt))

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

                self.server.info("Requesting RTT for %s" % self.nick)

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
    def __init__(self, fconf, ip, port, handler):
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

        self.groups = range(int(conf["num-groups"]))
        self.proc_per_group = int(conf["proc-per-group"])
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

        Server.__init__(self, ip, port, handler)

    def run(self):
        self.info("Server started ^C to stop it")
        self.info("Managing %d groups (%d procs per group) total: %d" % \
                  (len(self.groups), self.proc_per_group,
                   len(self.groups) * self.proc_per_group))

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
        self.dead_reduce_dict[nick] = lst

        if lst: # This might be None
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
        port = 8080
        server = MasterServer(fconf, '', port, Handler)
        server.run()
    except KeyboardInterrupt:
        print('^C received, shutting down server..')
        server.stop()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: %s <config-file>" % (sys.argv[0])
        sys.exit(-1)

    main(sys.argv[1])
