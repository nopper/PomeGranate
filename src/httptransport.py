"""
This module contains classes used for interacting with the http server
"""

import json
import socket
import StringIO
import mimetools
import threading

import asyncore

class HTTPRequest(object):
    """
    A simple class representing an HTTP response/request
    """
    def __init__(self, callback):
        self.callback = callback
        self.reset()

    def parse_headers(self, status, headers):
        """
        Called whenever the header terminator string is encountered
        @param status the HTTP status as 3-triple ('HTTP/1.1', '200', 'OK')
        @param headers a string representing the http headers
        """
        self.got_header = True
        self.protocol, self.reply_code, self.reply_status = status
        self.reply_code = int(self.reply_code)
        self.headers = headers

        try:
            self.clen = int(self.headers.getheader("content-length"))
        except:
            self.clen = 0

    def reset(self):
        "Reset the status of the request object"
        self.body = ""
        self.got_header = False
        self.headers = None
        self.buffer = []
        self.length = 0
        self.payload = ""

        self.protocol = ""
        self.reply_code = 400
        self.reply_status = ""

        self.headers = None
        self.clen = 0

    def feed(self, data):
        """
        Feed the request object with new data
        @param data a string representing new data read from a socket
        """
        needed = max(0, self.clen - len(self.payload))

        if needed == 0:
            self.callback(self)
            self.reset()
            return data
        else:
            tstr = data[:needed]
            self.buffer.append(tstr)
            self.length += len(tstr)

            if self.length == self.clen:
                buff, self.buffer = self.buffer, None
                self.payload = "".join(buff)

                self.callback(self)
                self.reset()

            return data[needed:]

    def get_reply(self):
        "@return the deserialzied JSON payload. Can throws exceptions"
        return json.loads(self.payload)

class HTTPClient(asyncore.dispatcher):
    """
    Simple asyncore http client
    """
    def __init__(self):
        asyncore.dispatcher.__init__(self)

        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__inbuffer = ""
        self.__request = HTTPRequest(self.__request_ready)
        self.__requests = []
        self.__requests_lock = threading.Lock()

    def run(self):
        asyncore.loop()

    def _add_request(self, request, immediate=False):
        """
        Used to push an HTTP request/response in the FIFO buffer or requests.
        The buffer will be flushed out periodically whenever it is possible.
        @param request a string representing your request
        @param immediate True if you want to put the request on top of the
                         queue
        """
        with self.__requests_lock:
            if immediate:
                self.__requests.insert(0, request)
            else:
                self.__requests.append(request)

    def __call(self, methname, *args):
        method = getattr(self, methname, None)

        if method is not None:
            method.__call__(*args)

    def __request_ready(self, request):
        """
        Callback triggered by the HTTPRequest object whenever a request is
        ready to be parsed.
        @param request the HTTPRequest object
        """
        if request.reply_code != 200:
            self.__call('_on_request_error', request)
        else:
            data = request.get_reply()
            self.__call('_on_' + data['type'].replace('-', '_'),
                        data['nick'], data['data'])

    ##########################################################################
    # Following methods are public but just because they need to be for the
    # asyncore API
    ##########################################################################

    def handle_connect(self):
        self.__call('_on_connected')

    def handle_write(self):
        with self.__requests_lock:
            if len(self.__requests) > 0:
                request = self.__requests[0]
                sent = self.send(request)

                if len(request) == sent:
                    self.__requests.pop(0)
                else:
                    self.__requests[0] = request[sent:]

    def handle_read(self):
        data = self.recv(2048)

        if not self.__request.got_header:
            self.__inbuffer += data
            buff = self.__inbuffer

            i = buff.find("\r\n\r\n")

            if i < 0:
                return

            fp = StringIO.StringIO(buff[:i + 4])
            self.__request.parse_headers(fp.readline().split(" ", 2),
                                         mimetools.Message(fp))

            self.__inbuffer = self.__request.feed(buff[i + 4:])
        else:
            self.__inbuffer = self.__request.feed(data)
