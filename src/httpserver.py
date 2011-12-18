"""Simple HTTP server based on the asyncore / asynchat framework

Under asyncore, every time a socket is created it enters a table which is
scanned through select calls by the asyncore.loop() function

All events (a client connecting to a server socket, a client sending data,
a server receiving data) is handled by the instances of classes derived
from asyncore.dispatcher

Here the server is represented by an instance of the Server class

When a client connects to it, its handle_accept() method creates an
instance of RequestHandler, one for each HTTP request. It is derived
from asynchat.async_chat, a class where incoming data on the connection
is processed when a "terminator" is received. The terminator can be :
- a string : here we'll use the string \r\n\r\n to handle the HTTP request
line and the HTTP headers
- an integer (n) : the data is processed when n bytes have been read. This
will be used for HTTP POST requests

The data is processed by a method called found_terminator. In RequestHandler,
found_terminator is first set to handle_request_line to handle the HTTP
request line (including the decoding of the query string) and the headers.
If the method is POST, terminator is set to the number of bytes to read
(the content-length header), and found_terminator is set to handle_post_data

After that, the handle_data() method is called and the connection is closed

Subclasses of RequestHandler only have to override the handle_data() method
"""

import asynchat, asyncore, socket, SimpleHTTPServer, select
import sys, cStringIO, traceback, shutil

class SocketStream:
    def __init__(self, sock, handler):
        """Initiate a socket (non-blocking) and a buffer"""
        self.sock = sock
        self.handler = handler
        self.buffer = cStringIO.StringIO()
        self.closed = 1

    def write(self, data):
        """Buffer the input, then send as many bytes as possible"""
        self.buffer.write(data)

        if self.writable():
            buff = self.buffer.getvalue()

            try:
                sent = self.sock.send(buff)
                self.buffer = cStringIO.StringIO()
                self.buffer.write(buff[sent:])
            except Exception:
                self.handler.handle_error()

    def finish(self):
        """When all data has been received, send what remains in the buffer"""
        data = self.buffer.getvalue()
        while len(data):
            while not self.writable():
                pass
            try:
                sent = self.sock.send(data)
                data = data[sent:]
            except Exception:
                self.handler.handle_error()

    def writable(self):
        """Used as a flag to know if something can be sent to the socket"""
        return select.select([], [self.sock], [])[1]

class RequestHandler(asynchat.async_chat,
    SimpleHTTPServer.SimpleHTTPRequestHandler):

    protocol_version = "HTTP/1.1"

    def __init__(self, conn, addr, server):
        asynchat.async_chat.__init__(self, conn)
        self.client_address = addr
        self.connection = conn
        self.server = server
        self.reset()

    def reset(self):
        # set the terminator : when it is received, this means that the
        # http request is complete ; control will be passed to
        # self.found_terminator
        self.set_terminator('\r\n\r\n')
        self.found_terminator = self.handle_request_line
        self.rfile = cStringIO.StringIO()
        self.request_version = "HTTP/1.1"
        # buffer the response and headers to avoid several calls to select()
        self.wfile = cStringIO.StringIO()

    def collect_incoming_data(self, data):
        """Collect the data arriving on the connexion"""
        self.rfile.write(data)

    def prepare_POST(self):
        """Prepare to read the request body"""
        bytesToRead = int(self.headers.getheader('content-length'))
        # set terminator to length (will read bytesToRead bytes)
        self.set_terminator(bytesToRead)
        self.rfile = cStringIO.StringIO()
        # control will be passed to a new found_terminator
        self.found_terminator = self.handle_post_data

    def handle_post_data(self):
        """Called when a POST request body has been read"""
        self.rfile.seek(0)
        self.do_POST()
        self.finish()

    def do_GET(self):
        pass

    def do_POST(self):
        pass

    def handle_request_line(self):
        """Called when the http request line and headers have been received"""
        # prepare attributes needed in parse_request()
        self.rfile.seek(0)
        self.raw_requestline = self.rfile.readline()
        self.parse_request()

        if self.command in ['GET','HEAD']:
            # if method is GET or HEAD, call do_GET or do_HEAD and finish
            method = "do_" + self.command
            if hasattr(self, method):
                getattr(self, method).__call__()
                self.finish()
        elif self.command == "POST":
            # if method is POST, call prepare_POST, don't finish yet
            self.prepare_POST()
        else:
            self.send_error(501, "Unsupported method (%s)" %self.command)

    def end_headers(self):
        """Send the blank line ending the MIME headers, send the buffered
        response and headers on the connection, then set self.wfile to
        this connection
        This is faster than sending the response line and each header
        separately because of the calls to select() in SocketStream"""
        if self.request_version != 'HTTP/0.9':
            self.wfile.write("\r\n")
        try:
            self.start_resp = cStringIO.StringIO(self.wfile.getvalue())
            self.wfile = SocketStream(self.connection, self)
            self.copyfile(self.start_resp, self.wfile)
        except Exception, exc:
            # FIXME please
            raise exc

    def copyfile(self, source, outputfile):
        """Copy all data between two file objects Set a big buffer size"""
        shutil.copyfileobj(source, outputfile, length = 128*1024)

    def handle_error(self):
        traceback.print_exc(sys.stderr)
        self.close()

    def finish(self):
        """Send data, then close"""
        try:
            self.wfile.finish()
        except AttributeError:
            # if end_headers() wasn't called, wfile is a StringIO
            # this happens for error 404 in self.send_head() for instance
            self.wfile.seek(0)
            self.copyfile(self.wfile, SocketStream(self.connection, self))

        self.reset()

class Server(asyncore.dispatcher):
    def __init__ (self, ip, port, handler):
        asyncore.dispatcher.__init__(self)

        self.ip = ip
        self.port = port
        self.handler = handler
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)

        self.set_reuse_addr()
        self.bind((ip, port))

        self.listen (1024)

    def handle_accept (self):
        try:
            conn, addr = self.accept()
        except socket.error:
            self.log_info('warning: server accept() threw an exception',
                          'warning')
            return
        except TypeError:
            self.log_info('warning: server accept() threw EWOULDBLOCK',
                          'warning')
            return

        self.handler(conn, addr, self)

    def run(self):
        asyncore.loop(timeout=2)
