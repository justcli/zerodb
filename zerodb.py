import sys
import json
import os
import time
import shutil
import atexit

global __flushfp
__flushfp = None

def cleanup(arg):
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


def timestamp() -> str:
    fmt = '%Y-%m-%d %H:%M:%S'
    return time.strftime(fmt)


def expired(stamp, age) -> int:
    from datetime import datetime, timedelta
    if not age:
        return 0
    n = unit = None
    fmt = '%Y-%m-%d %H:%M:%S'
    try:
        n = age[:-1]
        n = int(n)
        unit = age[-1:]
        n = int(n) if unit in ['s', 'm', 'h', 'd'] else n['n']
    except Exception:
        print('error expiry')
        return -1

    stamp = datetime.strptime(stamp, fmt)
    curr = datetime.now()
    if unit == 's':
        expiry = curr - timedelta(seconds=n)
    elif unit == 'm':
        expiry = curr - timedelta(minutes=n)
    elif unit == 'h':
        expiry = curr - timedelta(hours=n)
    elif unit == 'd':
        expiry = curr - timedelta(days=n)

    if stamp > expiry:
        return 0
    return 1


#def parse_cond(cond: str) -> dict:
#    '''
#    cond: str examples -
#    -   'name == Mohan'
#    -   'grade.students.passed == yes'
#    -   'name == 'A' and age == 10'
#    -   'name in [Ram, Mohan, Shyam]'
#    -   'name not in [John, Donald]'
#    '''
#    slice = cond
#    while True:
#        first = None
#        offset = 9999
#        jmp = 0
#        s_and = slice.find(' and ') + 1
#        if s_and:
#            first = 'and'
#            offset = s_and - 1
#            jmp = 5
#        s_or = slice.find(' or ') + 1
#        if s_or and s_or < offset:
#            first = 'or'
#            offset = s_or - 1
#            jmp = 4
#        s_in = slice.find(' in ') + 1
#        if s_in and s_in < offset:
#            first = 'in'
#            offset = s_in - 1
#            jmp = 4
#        s_notin = slice.find(' not in ') + 1
#        if s_notin and s_notin < offset:
#            first = 'not in'
#            offset = s_notin - 1
#            jmp = 8
#
#        if first is None:
#            return {}
#
#        left = slice[:offset]
#        slice = slice[offset + jmp:]
#        curr_cond['left'] = left
#        curr_cond['joint'] = first

class ZeroDB:

    def __init__(self, file=None, expiry=None):
        global old_exception_handler
        self._dbfile = file
        self._objlist = []
        self._objmap = {}
        self._locmap = {}
        self._adds = self._subs = 0
        self._reconciling = 0
        self._expiry = expiry

        if not file:
            return

        try:
            self._dbfp = open(self._dbfile, "a+")
            self._dbfp.seek(0)
        except Exception:
            print(f'Error opening the db file ({self._dbfile})', file=sys.stderr)
            exit(1)

        action = None
        atexit.register(cleanup, self)
        while line := self._dbfp.readline():
            if not action:
                action = line
                continue

            if expired(action[1:].strip(), self._expiry):
                action = None
                continue

            obj = json.loads(line.strip())
            alias = list(obj.keys())[0]
            value = obj[alias]
            try:
                n = self._objmap[alias]
            except KeyError:
                n = None
            if n is None and action[0] == '+':
                self._adds += 1
                d = {}
                d[alias] = [value]
                self._objlist.append(d)
                self._objmap[alias] = len(self._objlist) - 1
                self._locmap[alias] = self._dbfp.tell() - len(line)
            else:
                if action[0] == '+':
                    self._adds += 1
                    curr = self._objlist[n][alias]
                    curr.append(value)
                else:
                    self._subs += 1
                    self._objlist.pop(n)
                    del self._objmap[alias]
            action = None


    def insert(self, key: str, val: any):
        d = {}
        d[key] = [val]
        if key in self._objmap:
            n = self._objmap[key]
            self._objlist[n][key].append(val)
        else:
            self._objlist.append(d)
            self._objmap[key] = len(self._objlist) - 1
        if self._dbfile:
            txt = json.dumps(d)
            self._dbfp.write('+' + timestamp() + '\n' + txt + '\n')


    def remove(self, key: str):
        try:
            n = self._objmap[key]
        except KeyError:
            return
        d = self._objlist.pop(n)
        del self._objmap[key]
        if self._dbfile:
            self._dbfp.write('-\n' + json.dumps(d) + '\n')


    def query(self, key):
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

    def reconcile(self):
        self._bkup_file = os.path.dirname(self._dbfile) +\
                          '/' + self._dbfile + '.bkup'
        self._re_file = os.path.dirname(self._dbfile) +\
                        '/' + self._dbfile + '.tmp'
        self._reconciling = 1
        # reconcile removed entries
        if self._subs and (self._adds // self._subs) < 20:
            with open(self._re_file, 'w') as fp:
                for alias in self._objmap:
                    loc = self._locmap[alias]
                    self._dbfp.seek(loc, 0)
                    line = self._dbfp.readline()
                    line = '+\n' + line
                    fp.write(line)
        self._dbfp.close()
        if self._subs:
            try:
                shutil.copyfile(self._dbfile, self._bkup_file)
                shutil.copyfile(self._re_file, self._dbfile)
            except Exception:
                cleanup(self)
            if self._reconciling == 1:
                os.remove(self._bkup_file)
                os.remove(self._re_file)
        self._dbfp = open(self._dbfile, "a+")
        self._dbfp.seek(2)
        self._reconciling = 0




if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] != '-tidyup':
        print('Usage:\n> zerodb -tidyup <db filename>')
        exit(1)
 
    mydb = ZeroDB(sys.argv[2])
    s = time.time()
    mydb.reconcile()
    #mydb = ZeroDB('./mydb.zdb', expiry='1h')
    #e = time.time()
    #for i in range(200):
    #    d = {}
    #    d['mykey'] = 'myval'
    #    d['mylist'] = [1,2,3,4,5]
    #    alias = 'dictionary' + str(i)
        #mydb.remove(alias)
    #d = {'a':2, 'b': [2,2], 'c':'d'}
    #mydb.insert('mylist', d)
    #d = {'a':2, 'b': [2,2], 'c':'d'}
    #mydb.insert('mylist', d)
    #mydb.remove('mylist')
    d = mydb.query('key1')
    print(d)
    d = mydb.query('key2')
    print(d)
    print(s)
    print(e)

