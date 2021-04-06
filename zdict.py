#!/usr/bin/env python3
import sys
import os
import time
import atexit
import msgpack
from io import BytesIO
from tempfile import gettempdir

"""
Usage:
    mydict = Zdict("mydict_unique_name", restart_time=60).load()
    restart_time : If the app is not restarted before this time, the store
                   of the dict variable will be deleted.
                   If not specified, restart_time default value is 3600s
"""
__all__ = ['Zdict']

def _save_var(self):
    self._zdictfp = open(self._zdictfile, "wb")
    self._zdictfp.write(msgpack.packb(self._obj, use_bin_type=True))
    self._zdictfp.flush()
    self._zdictfp.close()


# Zdb-backed dict class
class Zdict(dict):
    def __init__(self, objname, restart_time=3600):
        self._obj = {}
        self._objname = objname
        self._zdictfile = gettempdir() + '/' + objname
        try:
            tm = os.stat(self._zdictfile).st_atime
            now = time.time()
            if now - tm > restart_time:
                # zdict file is more than 1hr old...dump it
                raise ValueError("Old zdict file")
            self._zdictfp = open(self._zdictfile, "rb")
        except Exception:
            self._zdictfp = open(self._zdictfile, "wb+") 
            if not self._zdictfp:
                raise ValueError("Unable to create backup file")
        self._zdictfp.close()
        atexit.register(_save_var, self)


    def load(self):
        val = None
        self._zdictfp = open(self._zdictfile, "rb")
        myio = BytesIO(self._zdictfp.read())
        it = msgpack.Unpacker(myio, raw=False)
        self._zdictfp.close()
        if not it:
            return self._obj
        for obj in it:
            val = obj
        if val:
            for k,v in val.items():
                self._obj.__setitem__(k, v)
        return self._obj

