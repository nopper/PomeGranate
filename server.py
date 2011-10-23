import sys
import json

from utils import Logger, load_module
from message import WorkerStatus, TYPE_MAP, TYPE_REDUCE

from threading import Lock
from collections import defaultdict
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(
            "<html><head><title>Status of the application</title></head>"
            "<body><h1>List of available machines:</h1><table>")

        with self.server.lock:
            masters = sorted(self.server.timestamps.items())

        self.wfile.write("".join(["<tr><td>%s</td><td>%d</td></tr>" % \
                                 (nick, time) for nick, time in masters]))
        self.wfile.write("</table></body>")


    def do_POST(self):
        clen = self.headers.getheader('content-length')
        request = json.loads(self.rfile.read(int(clen)))

        server = self.server
        server.lock.acquire()

        print "-" * 80
        print request
        print server.pending_works
        print

        # {type: str, nick: str, data: ..}

        if request['type'] == 'registration':
            nick = request['nick']
            server.masters.add(nick)

            self.send_response(200)
            self.end_headers()

            server.info("New group registered with the name %s" % nick)

        elif request['type'] == 'work-request':
            nick = request['nick']

            if not nick in server.masters:
                server.masters.add(nick)

            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()

            try:
                # If we have a work in the queue just assing it
                wstatus = server.work_queue.pop()
                server.pending_works[nick].append(wstatus)

                print server.pending_works

                server.info("Assigning work %s (tag: %s) to %s" % \
                            (str(wstatus.state), str(wstatus.tag), nick))

                if wstatus.type == TYPE_MAP:
                    msgtype = 'compute-map'
                else:
                    msgtype = 'compute-reduce'

                self.wfile.write(json.dumps({
                    "type": msgtype,
                    "nick": wstatus.tag,
                    "data": wstatus.state
                }))
            except StopIteration:
                if len(server.pending_works) == 0:
                    # If we have not any work nor pending works to acknowledge the
                    # computation is over so it's the perfect moment for a funeral

                    self.wfile.write(json.dumps({
                        "type": "plz-die",
                        "nick": '',
                        "data": '',
                    }))

                else:
                    # Otherwise just tell to the worker to retry in few moments

                    self.wfile.write(json.dumps({
                        "type": "try-later",
                        "nick": '',
                        "data": '',
                    }))

        elif request['type'] == 'map-completed':
            nick = request['nick']
            tag           = request['data'][0]
            reduce_pushed = request['data'][1]
            work          = request['data'][2]

            if nick not in server.masters:
                server.error("The group %s is not registered" % nick)
                self.send_response(400)
            else:
                jobs = server.pending_works[nick]
                found = False

                for pos, wstatus in enumerate(jobs):
                    if tag == wstatus.tag:
                        found = True
                        break

                if found:
                    if reduce_pushed == True:
                        jobs[pos].type = TYPE_REDUCE
                        jobs[pos].state = work
                    else:
                        del jobs[pos]

                    server.debug("Map acknowledged correctly for group %s" % \
                                 (nick))

                    print server.pending_works

                    self.send_response(200)
                else:
                    server.error("No work found with tag %d %s for group %s" % \
                                 (tag, str(jobs), nick))
                    print server.pending_works
                    self.send_response(400)

                self.send_header('Content-type', 'text/json')
                self.end_headers()


        elif request['type'] == 'reduce-completed':
            nick = request['nick']
            tag  = request['data'][0]
            work = request['data'][1]

            if nick not in server.masters:
                self.send_response(400)
            else:
                jobs = server.pending_works[nick]
                found = False

                for pos, wstatus in enumerate(jobs):
                    if tag == wstatus.tag:
                        found = True
                        break

                if found:
                    del jobs[pos]
                    self.send_response(200)
                else:
                    self.send_response(400)

        elif request['type'] == 'keep-alive':
            nick = request['nick']

            if nick not in server.masters:
                server.masters.add(nick)

            server.timestamps[nick] = 10

            # TODO: in caso abbiamo cambiato il numero di processi
            # dall'interfaccia web notifica
            self.send_response(200)
            self.send_header('Content-type', 'text/json')
            self.end_headers()

        server.lock.release()

class AbstractQueue(object):
    """
    This class is able to provide pop/append methods which are available for
    queues only on a generator object which does not supports it. It also
    provide thread-safety which is needed on a multi-threaded program.
    """
    def __init__(self, gen):
        self.lock = Lock()
        self.queue = []
        self.generator = gen
        self.last_tag = 0

    def next(self):
        with self.lock:
            self.last_tag += 1

            if len(self.queue) > 0:
                return WorkerStatus(
                    TYPE_REDUCE,
                    self.last_lag,
                    self.queue.pop()
                )

            return WorkerStatus(
                TYPE_MAP,
                self.last_tag,
                self.generator.next()
            )

    def append(self, obj):
        with self.lock:
            self.queue.append(obj)

    def pop(self):
        return self.next()

class Server(HTTPServer, Logger):
    def __init__(self, fconf, address, handler):
        Logger.__init__(self, "Manager")

        conf = json.load(open(fconf))

        self.groups = range(int(conf["num-groups"]))
        self.proc_per_group = int(conf["proc-per-group"])

        # Load the input module and assing the generator to a member
        module = load_module(conf["input-module"])
        cls = getattr(module, "Input", None)

        self.generator = cls(fconf).input()
        self.work_queue = AbstractQueue(self.generator)

        self.lock = Lock()
        self.masters = set()
        self.pending_works = defaultdict(list) # nick => [work, ...]
        self.timestamps = defaultdict(int)

        HTTPServer.__init__(self, address, handler)

        self.info("Server started %s" % str(address))
        self.info("^C to stop it")
        self.info("Managing %d groups (%d procs per group) total: %d" % \
                  (len(self.groups), self.proc_per_group,
                   len(self.groups) * self.proc_per_group))

    def hearthbeat(self):
        with self.lock:
            for nick in self.timestamps:
                self.timestamps[nick] -= 1

                if self.timestamps[nick] <= 0:
                    self.info("Master %s died" % nick)

                    for tag, work in self.pending_works[nick]:
                        self.work_queue.append(work)

                    del self.pending_works[nick]
                    del self.timestamps[nick]
                    self.masters.remove(nick)

def main(fconf):
    try:
        port = 8080
        server = Server(fconf, ('', port), Handler)
        server.serve_forever()
    except KeyboardInterrupt:
        print('^C received, shutting down server')
        server.socket.close()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: %s <config-file>" % (sys.argv[0])
        sys.exit(-1)

    main(sys.argv[1])
