import sys
import json
import atexit

global __flushfp
__flushfp = None

def cleanup(arg):
    import shutil
    import os
    if arg._reconciling:
        if os.path.exists(arg._bkup_file):
            try:
                arg._dbfile.close()
            except Exception:
                pass
            shutil.copy(arg._bkup_file, arg._dbfile)
            os.remove(arg._re_file)
    else:
        arg._dbfp.flush()
        arg._dbfp.close()


class ZeroDB:

    def __init__(self, dbfile):
        global old_exception_handler
        self._dbfile = dbfile
        self._objlist = []
        self._objmap = {}
        self._locmap = {}
        self._adds = self._subs = 0
        self._reconciling = 0
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
                self._adds += 1
                d = {}
                d[alias] = [value]
                self._objlist.append(d)
                self._objmap[alias] = len(self._objlist) - 1
                self._locmap[alias] = self._dbfp.tell() - len(line)
            else:
                if action == '+':
                    self._adds += 1
                    curr = self._objlist[n][alias]
                    curr.append(value)
                else:
                    self._subs += 1
                    self._objlist.pop(n)
                    del self._objmap[alias]
            action = None
        import shutil
        import os
        self._bkup_file = os.path.dirname(self._dbfile) + '/' + self._dbfile + '.bkup'
        self._re_file = os.path.dirname(self._dbfile) + '/' + self._dbfile + '.tmp'
        self._reconciling = 1
        self._reconcile_db(self._re_file)
        self._dbfp.close()
        try:
            shutil.copyfile(self._dbfile, self._bkup_file)
            shutil.copyfile(self._re_file, self._dbfile)
        except Exception:
            cleanup(self)
            self._reconciling = 2
        if self._reconciling == 1:
            os.remove(self._bkup_file)
            os.remove(self._re_file)
        self._dbfp = open(dbfile, "a+")
        self._dbfp.seek(2)
        self._reconciling = 0


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

    def _reconcile_db(self, refile):
        if self._subs and (self._adds // self._subs) > 20:
            return
        with open(refile, 'w') as fp:
            for alias in self._objmap:
                loc = self._locmap[alias]
                self._dbfp.seek(loc, 0)
                line = self._dbfp.readline()
                line = '+\n' + line
                fp.write(line)
        self._dbfp.seek(2)



if __name__ == '__main__':
    import time
    s = time.time()
    mydb = ZeroDB('./mydb.zdb')
    e = time.time()
    for i in range(200):
        d = {}
        d['mykey'] = 'myval'
        d['mylist'] = [1,2,3,4,5]
        alias = 'dictionary' + str(i)
        #mydb.remove(alias)
    #d = {'a':2, 'b': [2,2], 'c':'d'}
    #mydb.insert('mylist', d)
    #d = {'a':2, 'b': [2,2], 'c':'d'}
    #mydb.insert('mylist', d)
    #mydb.remove('mylist')
    d = mydb.query('mylist')
    print(d)
    print(s)
    print(e)

