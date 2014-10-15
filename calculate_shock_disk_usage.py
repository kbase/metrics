#!/usr/bin/env python

'''
Created on Oct 14, 2014

@author: gaprice@lbl.gov

Calculate shock disk usage and object counts  by user, separated into
public vs. private data.

These figures may not be actually related to the physical disk space since
shock has the ability to copy a node, which I believe makes the equivalent
of a hard link.

Don't run this during high loads - runs through every object in the DB
Hasn't been optimized much either.
'''
import math

# where to get credentials (don't check these into git, idiot)
CFG_FILE_DEFAULT = 'shock_usage.cfg'
CFG_SECTION_SOURCE = 'SourceMongo'
CFG_SECTION_TARGET = 'TargetMongo'

CFG_HOST = 'host'
CFG_PORT = 'port'
CFG_DB = 'db'
CFG_USER = 'user'
CFG_PWD = 'pwd'

# output file names
USER_FILE = 'user_data.json'
WS_FILE = 'ws_data.json'


class HexIterator(object):

    def __init__(self, width):
        self._width = width
        self._max = math.pow(16, width)
        self._current = 0

    def __iter__(self):
        return self

    def next(self):
        if self._current >= self._max:
            raise StopIteration

        h = "{0:0{1}x}".format(self._current, self._width)
        self._current += 1
        return h


if __name__ == '__main__':
    pass