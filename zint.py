#!/usr/bin/env python3
import os
import time
import atexit
import msgpack
from io import BytesIO
from tempfile import gettempdir

"""
Usage:
    myint = Zint("mydict_unique_name", restart_time=60).load()
    restart_time : If the app is not restarted before this time, the store
                   of the dict variable will be deleted.
                   If not specified, restart_time default value is 3600s
"""
__all__ = ['Zint']

def _save_var(self):
    self._zintfp = open(self._zintfile, "wb")
    self._zintfp.write(msgpack.packb(self._obj, use_bin_type=True))
    self._zintfp.flush()
    self._zintfp.close()


# Zdb-backed int class
class Zint(list):
    def __init__(self, objname, restart_time=3600):
        self._obj = []
        self._objname = objname
        self._zintfile = gettempdir() + '/' + objname
        try:
            tm = os.stat(self._zintfile).st_atime
            now = time.time()
            if now - tm > restart_time:
                # zint file is more than 1hr old...dump it
                raise ValueError("Old zint file")
            self._zintfp = open(self._zintfile, "rb")
        except Exception:
            self._zintfp = open(self._zintfile, "wb+") 
            if not self._zintfp:
                raise ValueError("Unable to create backup file")
        self._zintfp.close()
        atexit.register(_save_var, self)


    def load(self):
        self._zintfp = open(self._zintfile, "rb")
        myio = BytesIO(self._zintfp.read())
        it = msgpack.Unpacker(myio, raw=False)
        self._zintfp.close()
        if not it:
            return self._obj
        for obj in it:
            self._obj = obj
        return self._obj



