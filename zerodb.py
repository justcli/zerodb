import sys
import json

global __flushfp
__flushfp = None
print(f'__flushfp(global):{__flushfp} : {id(__flushfp)}')

def zerodb_excepthook(ex, msg, bt):
    global __flushfp, old_exception_handler
    print("Exception")
    # flush the db file
    if __flushfp:
        __flushfp.write(']')
        __flushfp.flush()
        __flushfp.close()
    old_exception_handler(ex, msg, bt)


class ZeroDB:

    def __init__(self, dbfile):
        global old_exception_handler
        old_exception_handler = sys.excepthook
        sys.excepthook = zerodb_excepthook
        self._objlist = []
        self._objmap = {}
        try:
            self._dbfp = open(dbfile, "a+")
            self._dbfp.seek(0)
            global __flushfp
            __flushfp = self._dbfp
            print(f'__flushfp:{__flushfp} : {id(__flushfp)}')
        except Exception:
            print(f'Error opening the db file {dbfile}', file=sys.stderr)
            return None

        action = None
        while line := self._dbfp.readline():
            if not action:
                action = line[0]
                continue
            obj = json.loads(line.strip())
            # every object in db is a key:value pair where key is the 
            # user given alias and value is the user object/data
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
                    #self._objlist[n][alias].remove(value)
                    self._objlist.pop(n)
                    del self._objmap[alias]
            action = None


    def insert(self, key: str, val: any):
        d = {}
        d[key] = val
        if key in self._objmap:
            n = self._objmap[key]
            print(self._objlist[n][key])
            self._objlist[n][key].append(val)
        else:
            self._objlist.append(d)
            self._objmap[key] = len(self._objlist) - 1
        # now write it to the file
        self._dbfp.write('+\n')
        txt = json.dumps(d)
        self._dbfp.write(txt + '\n')


    def remove(self, key: str):
        try:
            n = self._objmap[key]
        except KeyError:
            return
        d = self._objlist.pop(n)
        del self._objmap[key]
        self._dbfp.write('-\n')
        self._dbfp.write(json.dumps(d) + '\n')
        self._dbfp.flush()


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
    mydb = ZeroDB('./mydb.zdb')
    s = time.time()
    #d = mydb.remove('dictionary9999')
    #d = mydb.query('dictionary9999')
    #for i in range(1000000):
    #   d = {}
    #   d['mykey'] = 'myval'
    #   d['mylist'] = [1,2,3,4,5]
    #   alias = 'dictionary' + str(i)
    #   mydb.insert(alias, d)
    #d = {'a':1, 'b': [1,2], 'c':'a'}
    #mydb.insert('mylist', d)
    d = {'a':2, 'b': [2,2], 'c':'d'}
    mydb.insert('mylist', d)
    d = mydb.query('mylist')
    e = time.time()
    print(d[0])
    print(s)
    print(e)

