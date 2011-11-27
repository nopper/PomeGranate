#!/usr/bin/env python
# -*- coding: utf-8 -*-

# We retain the original copyright of gensim wikicorpus.py since this script
# file is just a readaption.

# Copyright (C) 2010 Radim Rehurek <radimrehurek@seznam.cz>
# Licensed under the GNU LGPL v2.1 - http://www.gnu.org/licenses/lgpl.html

"""
Simple module for parsing wikipedia dump and producing intermediate files for
our tf-idf calculation.

scripts $ time bzcat ~/enwiki-latest-pages-articles.xml.bz2 | python
          wiki-extractor.py /media/storage/collection/ 66060288

real	207m2.616s
user	231m51.573s
sys	3m8.468s

It took 3h and 27m to decompress the entire wikipedia dump (7GB) and to create
81 intermediate files (5.1GB cumulative output) on a Intel(R) Core(TM)2 Duo CPU
T7500 @2.20GHz. Totally 3504258 pages are taken in consideration.
"""

import re
import os
import sys
import zipfile
import tempfile

from xml.sax import make_parser
from xml.sax.handler import ContentHandler

ARTICLE_MIN_CHARS = 500
RE_HTML_ENTITY = re.compile(r'&(#?)(x?)(\w+);', re.UNICODE)

def to_unicode(text, encoding='utf8', errors='strict'):
    """Convert a string (bytestring in `encoding` or unicode), to unicode."""
    if isinstance(text, unicode):
        return text
    return unicode(text, encoding, errors=errors)

def decode_htmlentities(text):
    """
    Decode HTML entities in text, coded as hex, decimal or named.

    Adapted from http://github.com/sku/python-twitter-ircbot/blob/321d94e0e40d0acc92f5bf57d126b57369da70de/html_decode.py

    >>> u = u'E tu vivrai nel terrore - L&#x27;aldil&#xE0; (1981)'
    >>> print decode_htmlentities(u).encode('UTF-8')
    E tu vivrai nel terrore - L'aldilà (1981)
    >>> print decode_htmlentities("l&#39;eau")
    l'eau
    >>> print decode_htmlentities("foo &lt; bar")
    foo < bar

    """
    def substitute_entity(match):
        ent = match.group(3)
        if match.group(1) == "#":
            # decoding by number
            if match.group(2) == '':
                # number is in decimal
                return unichr(int(ent))
            elif match.group(2) == 'x':
                # number is in hex
                return unichr(int('0x' + ent, 16))
        else:
            # they were using a name
            cp = n2cp.get(ent)
            if cp:
                return unichr(cp)
            else:
                return match.group()

    try:
        return RE_HTML_ENTITY.sub(substitute_entity, text)
    except:
        # in case of errors, return input
        # e.g., ValueError: unichr() arg not in range(0x10000) (narrow Python build)
        return text


RE_P0 = re.compile('<!--.*?-->', re.DOTALL | re.UNICODE)
RE_P1 = re.compile('<ref([> ].*?)(</ref>|/>)', re.DOTALL | re.UNICODE)
RE_P2 = re.compile("(\n\[\[[a-z][a-z][\w-]*:[^:\]]+\]\])+$", re.UNICODE)
RE_P3 = re.compile("{{([^}{]*)}}", re.DOTALL | re.UNICODE)
RE_P4 = re.compile("{{([^}]*)}}", re.DOTALL | re.UNICODE)
RE_P5 = re.compile('\[(\w+):\/\/(.*?)(( (.*?))|())\]', re.UNICODE)
RE_P6 = re.compile("\[([^][]*)\|([^][]*)\]", re.DOTALL | re.UNICODE)
RE_P7 = re.compile('\n\[\[[iI]mage(.*?)(\|.*?)*\|(.*?)\]\]', re.UNICODE)
RE_P8 = re.compile('\n\[\[[fF]ile(.*?)(\|.*?)*\|(.*?)\]\]', re.UNICODE)
RE_P9 = re.compile('<nowiki([> ].*?)(</nowiki>|/>)', re.DOTALL | re.UNICODE)
RE_P10 = re.compile('<math([> ].*?)(</math>|/>)', re.DOTALL | re.UNICODE)
RE_P11 = re.compile('<(.*?)>', re.DOTALL | re.UNICODE)
RE_P12 = re.compile('\n(({\|)|(\|-)|(\|}))(.*?)(?=\n)', re.UNICODE)
RE_P13 = re.compile('\n(\||\!)(.*?\|)*([^|]*?)', re.UNICODE)

def filter_wiki(raw):
    """
    Filter out wiki mark-up from `raw`, leaving only text. `raw` is either unicode
    or utf-8 encoded string.
    """
    # parsing of the wiki markup is not perfect, but sufficient for our purposes
    # contributions to improving this code are welcome :)
    text = decode_htmlentities(to_unicode(raw, 'utf8', errors='ignore'))
    text = decode_htmlentities(text) # '&amp;nbsp;' --> '\xa0'
    text = re.sub(RE_P2, "", text) # remove the last list (=languages)
    # the wiki markup is recursive (markup inside markup etc)
    # instead of writing a recursive grammar, here we deal with that by removing
    # markup in a loop, starting with inner-most expressions and working outwards,
    # for as long as something changes.
    iters = 0
    while True:
        old, iters = text, iters + 1
        text = re.sub(RE_P0, "", text) # remove comments
        text = re.sub(RE_P1, '', text) # remove footnotes
        text = re.sub(RE_P9, "", text) # remove outside links
        text = re.sub(RE_P10, "", text) # remove math content
        text = re.sub(RE_P11, "", text) # remove all remaining tags
        # remove templates (no recursion)
        text = re.sub(RE_P3, '', text)
        text = re.sub(RE_P4, '', text)
        text = re.sub(RE_P5, '\\3', text) # remove urls, keep description
        text = re.sub(RE_P7, '\n\\3', text) # simplify images, keep description only
        text = re.sub(RE_P8, '\n\\3', text) # simplify files, keep description only
        text = re.sub(RE_P6, '\\2', text) # simplify links, keep description only
        # remove table markup
        text = text.replace('||', '\n|') # each table cell on a separate line
        text = re.sub(RE_P12, '\n', text) # remove formatting lines
        text = re.sub(RE_P13, '\n\\3', text) # leave only cell content
        # remove empty mark-up
        text = text.replace('[]', '')
        if old == text or iters > 2: # stop if nothing changed between two iterations or after a fixed number of iterations
            break

    # the following is needed to make the tokenizer see '[[socialist]]s' as a single word 'socialists'
    # TODO is this really desirable?
    text = text.replace('[', '').replace(']', '') # promote all remaining markup to plain text
    return text

class WikiPageHandler(ContentHandler):
    def __init__(self, output_dir, threshold):
        ContentHandler.__init__(self)

        self.level = 0
        self.in_title = True
        self.title = None

        self.num_articles = 0
        self.collected = 0
        self.num_outputs = 0

        self.threshold = threshold
        self.buffer = []
        self.file = None
        self.output_dir = os.path.abspath(output_dir)

        self.new_file()

    def startElement(self, name, attrs):
        if name == "text":
            self.level += 1
        if name == "title" and self.level == 0:
            self.in_title = True

    def endElement(self, name):
        if name == "text":
            self.level -= 1

            if self.level == 0:
                self.filter_wiki()
        if name == "title" and self.level == 0:
            self.in_title = False

    def characters(self, ch):
        if self.level == 0 and self.in_title:
            self.title = ch
        elif self.level > 0:
            self.buffer.append(ch)

    def filter_wiki(self):
        text = filter_wiki(''.join(self.buffer))
        self.buffer = []

        if len(text) > ARTICLE_MIN_CHARS:
            self.num_articles += 1
            self.collected += 1

            filename  = "doc-{:d}-{:s}".format(self.num_articles,
                                               self.title.encode("utf-8"))
            self.file.writestr(filename, text.encode("utf-8"))

            if self.file.fp.tell() > self.threshold:
                self.close_file()
                self.new_file()

    def new_file(self):
        handle = tempfile.NamedTemporaryFile(prefix='out-', suffix='.zip',
                                             dir=self.output_dir, delete=False)
        self.file = zipfile.ZipFile(handle.name, mode='w',
                                    compression=zipfile.ZIP_DEFLATED)

    def close_file(self):
        self.file.close()

        if self.collected == 0:
            os.unlink(self.file.filename)
            return

        dst = os.path.join(self.output_dir, 'coll-{:06d}-{:06d}.zip'.format(
                           self.num_outputs, self.collected))
        os.rename(self.file.filename, dst)
        self.num_outputs += 1
        self.collected = 0

def main():
    if len(sys.argv) != 3:
        print("Usage: {:s} <output-dir> " \
              "<bytes-soft-threshold>".format(sys.argv[0]))
        sys.exit(-1)

    parser = make_parser()
    handler = WikiPageHandler(sys.argv[1], int(sys.argv[2]))
    parser.setContentHandler(handler)

    try:
        parser.parse(sys.stdin)
    finally:
        handler.close_file()

if __name__ == "__main__":
    main()
