#!/usr/bin/env python3
import os
import time
import sys
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
    if not self._active:
        return
    self._zintfp = open(self._zintfile, "wb")
    self._zintfp.write(msgpack.packb(self._obj, use_bin_type=True))
    self._zintfp.flush()
    self._zintfp.close()


# Zdb-backed int class
class Zint():
    def __init__(self, objname, default=0, restart_time=3600):
        if not isinstance(default, int):
            raise ValueError("Default value is must be int, "
                             f"{type(default)} given")
        self._obj = default
        self._objname = objname
        self._active = 1
        self._restart_time = restart_time
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
            return self
        for obj in it:
            self._obj = obj
        return self

    def __disable(self):
        self._active = 0
        self._zintfp.close()

    def __set(self, val):
        self._obj = val

    def __str__(self):
        return str(self._obj)

    def __add__(self, b):
        self._obj = self._obj + b
        return self

    def __iadd__(self, b):
        self._obj = self._obj + b
        return self

    def __sub__(self, b):
        self._obj = self._obj - b
        return self

    def __isub__(self, b):
        self._obj = self._obj - b
        return self

    def __mul__(self, b):
        self._obj = self._obj * b
        return self

    def __imul__(self, b):
        self._obj = self._obj * b
        return self

    def __div__(self, b):
        self._obj = self._obj / b
        return self

    def __idiv__(self, b):
        self._obj = self._obj / b
        return self

    def __floordiv__(self, b):
        self._obj = self._obj // b
        return self

    def __ifloordiv__(self, b):
        self._obj = self._obj // b
        return self

    def __mod__(self, b):
        self._obj = self._obj % b
        return self
        #self.__disable()
        #n = Zint(self._objname, restart_time=self._restart_time).load()
        #n.__set(self._obj % b)
        #return n

    def __imod__(self, b):
        self._obj = self._obj % b
        return self

    def __repr__(self):
        return f"Zint[intvalue: {self._obj}, file:{self._zintfile}]"

