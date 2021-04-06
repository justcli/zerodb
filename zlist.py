#!/usr/bin/env python3
import os
import time
import atexit
import msgpack
from io import BytesIO
from tempfile import gettempdir

"""
Usage:
    mylist = Zlist("mydict_unique_name", restart_time=60).load()
    restart_time : If the app is not restarted before this time, the store
                   of the dict variable will be deleted.
                   If not specified, restart_time default value is 3600s
"""
__all__ = ['Zlist']

def _save_var(self):
    self._zlistfp = open(self._zlistfile, "wb")
    self._zlistfp.write(msgpack.packb(self._obj, use_bin_type=True))
    self._zlistfp.flush()
    self._zlistfp.close()


# Zdb-backed list class
class Zlist(list):
    def __init__(self, objname, restart_time=3600):
        self._obj = []
        self._objname = objname
        self._zlistfile = gettempdir() + '/' + objname
        try:
            tm = os.stat(self._zlistfile).st_atime
            now = time.time()
            if now - tm > restart_time:
                # zlist file is more than 1hr old...dump it
                raise ValueError("Old zlist file")
            self._zlistfp = open(self._zlistfile, "rb")
        except Exception:
            self._zlistfp = open(self._zlistfile, "wb+") 
            if not self._zlistfp:
                raise ValueError("Unable to create backup file")
        self._zlistfp.close()
        atexit.register(_save_var, self)


    def load(self):
        self._zlistfp = open(self._zlistfile, "rb")
        myio = BytesIO(self._zlistfp.read())
        it = msgpack.Unpacker(myio, raw=False)
        self._zlistfp.close()
        if not it:
            return self._obj
        for obj in it:
            self._obj = obj
        return self._obj



