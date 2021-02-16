#!/usr/bin/env python3
import sys
import json
import os
import time
import shutil
import atexit


def cleanup(arg):
    if arg._dbfp:
        arg._dbfp.flush()
        arg._dbfp.close()


def timestamp() -> str:
    fmt = '%Y-%m-%d %H:%M:%S'
    return time.strftime(fmt)


def expired(stamp, age) -> int:
    from datetime import datetime, timedelta
    try:
        if not int(age[:-1]):
            return 0
    except Exception:
        return -1
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


class ZeroDB:

    def __init__(self, file=None, expiry=None):
        global old_exception_handler
        self._dbfile = file
        self._objlist = []
        self._objmap = {}
        self._locmap = {}
        self._adds = self._subs = 0
        self._expiry = '0h' if not expiry else expiry

        if not file:
            return
        self._dbfile = os.path.expanduser(file)

        try:
            self._dbfp = open(self._dbfile, "a+")
            self._dbfp.seek(0)
            line = self._dbfp.readline()
            if not line:
                self._dbfp.write(self._expiry + '\n')
            else:
                self._expiry = line.strip()
        except Exception:
            print(f'Error opening the db file ({self._dbfile})',
                  file=sys.stderr)
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
        try:
            x = (self._subs * 100) // (self._adds + self._subs)
            if x > 20:
                print(f'{os.path.basename(self._dbfile)} can be compacted by \
                      more than {x}%. You may run "zerodb -tidyup \
                      <db filename>"')
        except Exception:
            pass



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
        if self._dbfile:
            self._dbfp.flush()


    def tidyup(self, outfile=sys.stdout):
        # tidyup removed entries
        if not self._dbfile:
            return
        self._dbfp.seek(0)
        expiry = self._dbfp.readline().strip()
        outfile.write(expiry + '\n')
        action = None
        while line := self._dbfp.readline():
            if not action:
                action = line
                continue
            if action[0] == '-':
                action = None
                continue
            if action[0] == '+':
                obj = json.loads(line.strip())
                alias = list(obj.keys())[0]
                if alias not in self._objmap:
                    action = None
                    continue
                if not expired(action[1:].strip(), expiry):
                    outfile.write(action + line)
            action = None
        self._dbfp.close()
        self._dbfp = None



if __name__ == '__main__':
    '''
    zerodb -tidyup <db file>  [<output file>]
    '''
    if len(sys.argv) == 2 and sys.argv[1] == '-benchmark':
        mydb = ZeroDB()
        s = time.time()
        nr = 100000
        for i in range(nr):
            d = {'a': 'aaaaa', 'b': [1,2,3,4,5]}
            mydb.insert('key' + str(i), d)
        e = time.time()
        diff = float(e) - float(s)
        print('In-memory : ' + str(int(nr // diff)) + ' inserts / sec')
        mydb = ZeroDB('mydb.zdb')
        s = time.time()
        nr = 100000
        for i in range(nr):
            d = {'a': 'aaaaa', 'b': [1,2,3,4,5]}
            mydb.insert('key' + str(i), d)
        e = time.time()
        diff = float(e) - float(s)
        print('Storage   : ' + str(int(nr // diff)) + ' inserts / sec')

    else:

        if len(sys.argv) < 3 or len(sys.argv) > 4 or sys.argv[1] != '-tidyup':
            print('Usage:\n> zerodb -tidyup <db filename> <output filename>')
            exit(1)

        try:
            mydb = ZeroDB(os.path.abspath(sys.argv[2]))
            dir = None
            fp = sys.stdout
            if len(sys.argv) == 4:
                fp = open(sys.argv[3], 'w+')
                #dir = os.path.dirname(os.path.abspath(sys.argv[3]))
            mydb.tidyup(fp)
            fp.close()
        except Exception as e:
            print(e, file=sys.stderr)
            exit(1)
    exit(0)

