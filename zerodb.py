import sys
import json
import atexit

global __flushfp
__flushfp = None

def cleanup(arg):
    arg._dbfp.flush()
    arg._dbfp.close()


class ZeroDB:

    def __init__(self, dbfile):
        global old_exception_handler
        self._objlist = []
        self._objmap = {}
        atexit.register(cleanup, self)

        try:
            self._dbfp = open(dbfile, "a+")
            self._dbfp.seek(0)
        except Exception:
            print(f'Error opening the db file {dbfile}', file=sys.stderr)
            return None

        action = None
        while line := self._dbfp.readline():
            if not action:
                action = line[0]
                continue
            obj = json.loads(line.strip())
            alias = list(obj.keys())[0]
            value = obj[alias]
            try:
                n = self._objmap[alias]
            except KeyError:
                n = None
            if n is None and action == '+':
                d = {}
                d[alias] = [value]
                self._objlist.append(d)
                self._objmap[alias] = len(self._objlist) - 1
            else:
                if action == '+':
                    curr = self._objlist[n][alias]
                    curr.append(value)
                else:
                    self._objlist.pop(n)
                    del self._objmap[alias]
            action = None

        global __flushfp
        __flushfp = self._dbfp


    def insert(self, key: str, val: any):
        d = {}
        d[key] = [val]
        if key in self._objmap:
            n = self._objmap[key]
            self._objlist[n][key].append(val)
        else:
            self._objlist.append(d)
            self._objmap[key] = len(self._objlist) - 1
        txt = json.dumps(d)
        self._dbfp.write('+\n' + txt + '\n')


    def remove(self, key: str):
        try:
            n = self._objmap[key]
        except KeyError:
            return
        d = self._objlist.pop(n)
        del self._objmap[key]
        self._dbfp.write('-\n' + json.dumps(d) + '\n')


    def query(self, key: str):
        try:
            n = self._objmap[key]
        except KeyError:
            return None
        obj = self._objlist[n][key]
        if len(obj) == 1:
            obj = obj[0]
        return obj


    def flush(self):
        self._dbfp.flush()



if __name__ == '__main__':
    import time
    s = time.time()
    mydb = ZeroDB('./mydb.zdb')
    #for i in range(500000):
    #   d = {}
    #   d['mykey'] = 'myval'
    #   d['mylist'] = [1,2,3,4,5]
    #   alias = 'dictionary' + str(i)
    #   mydb.insert(alias, d)
    e = time.time()
    #d = {'a':2, 'b': [2,2], 'c':'d'}
    #mydb.insert('mylist', d)
    #d = {'a':2, 'b': [2,2], 'c':'d'}
    #mydb.insert('mylist', d)
    #mydb.remove('mylist')
    d = mydb.query('mylist')
    print(d)
    print(s)
    print(e)

