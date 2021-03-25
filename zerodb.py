#!/usr/bin/env python3
import re
import sys
import json
import os
import time
import shutil
import datetime
import atexit
import msgpack
from io import BytesIO

__all__ = ['ZeroDB']

global logfp

def error(msg):
    global logfp
    try:
        logfp
    except NameError:
        logfp = open('log_zerodb.txt', 'a+')
    t = datetime.datetime.now().strftime("%H:%M:%S.%f")
    msg = t + ': ' + str(msg)
    print(msg, file=logfp)
    if __name__ == '__main__':
        print(msg, file=sys.stderr)



def show_help():
    print('Usage  :\n'
          '       zerodb [-t|-d [<key-pattern>]|'
          '-q [select=<key> where=<condition>]|-b|-k] '
          '[<db filename>] {<output filename>}\n'
          '       -t : tidyup or compact the database '
          '(needs output filename)\n'
          '       -d : dump the values stored against a given key '
          'or key-pattern\n'
          '       -q : query the values of the given key and select '
          'ones matching the condition\n'
          '       -b : benchmark ZeroDB\n'
          '       -k : list all keys in the database\n'
          'Example:\n'
          '       > zerodb -t mydb newdb\n'
          '       > zerodb -d mykey mydb\n'
          '       > zerodb -q \'select mykey where .["grade"] > 5\'\n'
          '       > zerodb -k mydb\n'
          '       > zerodb -b')



def compile_n_run(datalist, cond):
    output = []
    cond = cond.replace('.[', 'e[')
    try:
        code = ('for e in datalist:\n'
                + '    if(' + cond + '):\n'
                + '        output.append(e)\n'
                )
        pycode = compile(code, '', 'exec')
        exec(pycode)
    except Exception as e:
        error(e)
    return output



def cleanup(arg):
    if arg._dbfp:
        arg._dbfp.flush()
        arg._dbfp.close()
    try:
        logfp
        logfp.flush()
        logfp.close()
    except Exception:
        pass



def dump_raw(dbfile):
    dbfile = os.path.expanduser(dbfile)
    if not dbfile.endswith('.zdb'):
        dbfile += '.zdb'

    try:
        dbfp = open(dbfile, "rb")
        dbfp.seek(0)
    except Exception:
        error(f'Error opening the db file ({dbfile})')
        exit(1)

    myio = BytesIO(dbfp.read())
    it = msgpack.Unpacker(myio, raw=False)
    for obj in it:
        print(obj)



def timestamp() -> str:
    fmt = '%Y-%m-%d %H:%M:%S'
    return time.strftime(fmt)



def remake_map(self):
    self._objmap.clear()
    i = 0
    for e in self._objlist:
        self._objmap[list(e.keys())[0]] = i
        i += 1



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
        error('expiry not in n[s|m|h|d] format.')
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
        self._adds = self._subs = 0
        self._expiry = '0h' if not expiry else expiry

        if not file:
            return
        self._dbfile = os.path.expanduser(file)
        if not self._dbfile.endswith('.zdb'):
            self._dbfile += '.zdb'

        try:
            self._dbfp = open(self._dbfile, "ab+")
            self._dbfp.seek(0)
        except Exception:
            error(f'Error opening the db file ({self._dbfile})')
            exit(1)

        atexit.register(cleanup, self)
        myio = BytesIO(self._dbfp.read())
        it = msgpack.Unpacker(myio, raw=False)
        for obj in it:
            if expired(obj['t'], self._expiry):
                continue
            alias = obj['k']
            if obj['a'] == '+':
                d = {}
                try:
                    n = self._objmap[alias]
                    self._objlist[n][alias].append(obj['v'])
                except KeyError:
                    #alias = obj['k']
                    d[alias] = [obj['v']]
                    self._objlist.append(d)
                    #self._objmap[alias] = len(self._objlist) - 1
                    remake_map(self)
                    self._adds += 1
            else:
                n = self._objmap[alias]
                #alias = obj['k']
                self._objlist.pop(n)
                remake_map(self)
                #self._objmap.pop(alias)
                self._subs += 1
        try:
            x = (self._subs * 100) // (self._adds + self._subs)
            if x > 20:
                print('This database can be compacted by '
                      + f'more than {x}%. '
                      + 'You may run "zerodb -tidyup '
                      + f'{os.path.basename(self._dbfile)}"',
                      file=sys.stderr)
        except Exception:
            pass



    def insert(self, key: str, val: any):
        if key in self._objmap:
            n = self._objmap[key]
            self._objlist[n][key].append(val)
        else:
            dat = {}
            dat[key] = [val]
            self._objlist.append(dat)
            self._objmap[key] = len(self._objlist) - 1
        if self._dbfile:
            d = {}
            d['a'] = '+'
            d['k'] = key
            d['v'] = val
            d['t'] = timestamp()
            self._dbfp.write(msgpack.packb(d, use_bin_type=True))



    def remove(self, key: str):
        try:
            n = self._objmap[key]
        except KeyError:
            return
        self._objlist.pop(n)
        remake_map(self)
        #del self._objmap[key]
        d = {}
        d['k'] = key
        #d['v'] = obj[key]
        d['a'] = '-'
        d['t'] = timestamp()
        if self._dbfile:
            self._dbfp.write(msgpack.packb(d, use_bin_type=True))



    def query(self, select, where=None):
        '''
        Query the database with args:
            select: name of the key
            where : query to run against the value(s) of the matching keys
        Example:
            query('orders', '.["side"] == "buy"')
            query('students', '.['marks'] > 90 and .['name'].startswith("a"))
        '''
        try:
            n = self._objmap[select]
        except KeyError:
            return []
        try:
            obj = self._objlist[n][select]
        except Exception as e:
            error(e)
            return []
        if len(obj) == 1:
            obj = obj[0]

        if not where:
            return obj
        return compile_n_run(obj, where) 



    def flush(self):
        if self._dbfile:
            self._dbfp.flush()



    def count(self, key=None) -> int:
        if not key:
            return len(self._objlist)
        if key not in self._objmap:
            return 0
        n = self._objmap[key]
        return len(self._objlist[n][key])



    def close(self):
        cleanup(self)



    def keys(self, pattern='*') -> list:
        if not pattern:
            return []
        pattern = str(pattern)
        if pattern.startswith('*'):
            pattern = pattern.replace('*', '[a-zA-Z0-9]', 1)
        keys = []
        for key in self._objmap:
            if re.match(pattern, str(key)):
                keys.append(key)
        return keys



    def tidyup(self, outfile):
        # tidyup removed entries
        if not self._dbfile:
            return
        self._dbfp.seek(0)
        curr_list = []
        filtered = []
        myio = BytesIO(self._dbfp.read())
        it = msgpack.Unpacker(myio, raw=False)
        for obj in it:
            if expired(obj['t'], self._expiry):
                continue
            if obj['a'] == '+':
                alias = obj['k']
                if alias not in curr_list:
                    filtered.append(obj)
                    curr_list[alias] = len(filtered) - 1
            elif obj['a'] == '-':
                alias = obj['k']
                if alias not in self._objmap:
                    continue
                filtered.remove(obj)
                curr_list.remove(alias)

        # now dump the filtered data in the outfile
        with open(outfile, 'wb+') as fp:
            for obj in filtered:
                fp.write(msgpack.packb(obj, use_bin_type=True))



if __name__ == '__main__':
    '''
    zerodb -tidyup <db file>  [<output file>]
    '''
    import gc
    gc.disable()

    # do the benchmarking
    if len(sys.argv) == 2 and sys.argv[1] == '-b':
        from random import randrange
        mydb = ZeroDB()
        s = time.time()
        nr = 1000000
        for i in range(nr):
            d = {'a': i}
            mydb.insert('key' + str(i), d)
        e = time.time()
        diff = float(e) - float(s)
        print('In-memory : ' + str(int(nr // diff)) + ' inserts / sec')
        gc.collect()
        s = time.time()
        for i in range(nr):
            mydb.query('key' + str(randrange(nr - 1)))
        e = time.time()
        qdiff = float(e) - float(s)
        gc.collect()
        mydb = ZeroDB('zerodb.zdb')
        s = time.time()
        for i in range(nr):
            d = {'a': i}
            mydb.insert('key' + str(i), d)
        e = time.time()
        diff = float(e) - float(s)
        print('Storage   : ' + str(int(nr // diff)) + ' inserts / sec')
        print('Query     : ' + str(int(nr // qdiff)) + ' queries / sec')
        mydb.close()
        mydb._dbfp = None
        os.unlink('zerodb.zdb')

    # dump the key and value of all database entries
    elif len(sys.argv) == 3 and sys.argv[1] == '-d':
        # zerodb -d mykey mydb
        mydb = ZeroDB(sys.argv[2])
        keys = mydb.keys('*')
        for key in keys:
            print(f"key: {key}")
            vals = mydb.query(key)
            if isinstance(vals, list):
                for val in vals:
                    print(val)
            else:
                print(vals)

    # query the database
    elif len(sys.argv) == 4 and sys.argv[1] == '-q':
        # zerodb -q 'select mykey where .["name"] == "myname"' mydb
        mydb = ZeroDB(sys.argv[3])
        if not mydb:
            error("Could not open the database")
            exit(1)

        q = sys.argv[2].split(' ')
        if q[0] != 'select' or q[2] != 'where':
            show_help()
            exit(1)
        key = q[1]
        cond = ' '.join(q[3:])

        out = mydb.query(key, where=cond)
        print(out)

    # tidy-up the database
    elif len(sys.argv) == 3 and sys.argv[1] == '-t':
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

    # dump the whole database in raw format
    elif len(sys.argv) == 3 and sys.argv[1] == '-r':
        # print the raw data of the database
        dump_raw(sys.argv[2])

    # list all the keys of the database
    elif len(sys.argv) == 3 and sys.argv[1] == '-k':
        mydb = ZeroDB(sys.argv[2])
        if not mydb:
            error("Could not open the database file")
            exit(1)
        keys = mydb.keys()
        print(keys)

    else:
        show_help()
        exit(1)

    exit(0)

# vim : set ts=4 shiftwidth=4 expandtab ffs=unix
