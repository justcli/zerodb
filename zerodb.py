#!/usr/bin/env python3
import sys
import json
import os
import time
import shutil
import atexit
import msgpack
from io import BytesIO
import re

__all__ = ['ZeroDB']

def compile_n_run(raw, condition, dump=False):
    rsp = {
        'retcode': 0,
        'msg' : None
    }
    try:
        rsp_dict = json.loads(raw)
        code = ('import os\n'
                + 'raw=' + str(rsp_dict) + '\n'
                + 'data=' + str(rsp_dict['data']) + '\n'
                + 'header=' + str(rsp_dict['header']) + '\n'
                + 'status_code=' + '\"' + str(rsp_dict['status_code']) + '\"\n'
                + 'reason_phrase=' + '\"'
                                + str(rsp_dict['reason_phrase']) + '\"\n'
                + 'http_version=' + '\"' + str(rsp_dict['http_version'])
                                + '\"\n'
                + 'os.environ[\'unicorn\']=str(' + condition + ')\n')
        print(code)
        pycode = compile(code, '', 'exec')
        exec(pycode)
        res = os.environ['unicorn']
        rsp['retcode'] = 0
        if not dump and (res == 'False'):
            rsp['retcode'] = -1
        if dump:
            rsp['msg'] = res
    except Exception as e:
        rsp['retcode'] = -1
        rsp['msg'] = str(e)
    return rsp


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
        self._adds = self._subs = 0
        self._expiry = '0h' if not expiry else expiry

        if not file:
            return
        self._dbfile = os.path.expanduser(file)

        try:
            self._dbfp = open(self._dbfile, "ab+")
            self._dbfp.seek(0)
        except Exception:
            print(f'Error opening the db file ({self._dbfile})',
                  file=sys.stderr)
            exit(1)

        atexit.register(cleanup, self)
        myio = BytesIO(self._dbfp.read())
        it = msgpack.Unpacker(myio, raw=False)
        for obj in it:
            if expired(obj['t'], self._expiry):
                continue
            if obj['a'] == '+':
                d = {}
                alias = obj['k']
                d[alias] = [obj['v']]
                try:
                    n = self._objmap[alias]
                    self._objlist[n][alias].append(d)
                except KeyError:
                    self._objlist.append(d)
                    self._objmap[alias] = len(self._objlist) - 1
                    self._adds += 1
            else:
                n = self._objmap[alias]
                alias = obj['k']
                self._objlist.pop(n)
                self._subs += 1
        try:
            x = (self._subs * 100) // (self._adds + self._subs)
            if x > 20:
                print(f'{os.path.basename(self._dbfile)} can be compacted by \
                      more than {x}%. You may run "zerodb -tidyup \
                      <db filename>"')
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
            d['k'] = key
            d['v'] = val
            d['a'] = '+'
            d['t'] = timestamp()
            self._dbfp.write(msgpack.packb(d, use_bin_type=True))



    def remove(self, key: str):
        try:
            n = self._objmap[key]
        except KeyError:
            return
        d = self._objlist.pop(n)
        del self._objmap[key]
        #d = {}
        d['a'] = '-'
        d['t'] = timestamp()
        if self._dbfile:
            self._dbfp.write(msgpack.packb(d, use_bin_type=True))



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



    def count(self, key=None) -> int:
        if not key:
            return len(self._objlist)
        if key not in self._objmap:
            return 0
        n = self._objmap[key]
        return len(self._objlist[n][key])



    def keys(self, like='*'):
        keylist = []
        if like == '':
            return keylist
        elif like.startswith('*'):
            like = ''.join(['[a-zA-Z0-9]', like[1:]])

        for key in self._objmap:
            if re.match(like, key):
                keylist.append(key)
        return keylist



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
        self._dbfp.close()
        self._dbfp = None



if __name__ == '__main__':
    '''
    zerodb -tidyup <db file>  [<output file>]
    '''
    import gc
    gc.disable()
    if len(sys.argv) == 2 and sys.argv[1] == '-benchmark':
        mydb = ZeroDB()
        s = time.time()
        nr = 1000000
        for i in range(nr):
            d = {'a': i}
            mydb.insert('key' + str(i), d)
        e = time.time()
        diff = float(e) - float(s)
        print('In-memory : ' + str(int(nr // diff)) + ' inserts / sec')
        mydb = ZeroDB('mydb.zdb')
        s = time.time()
        for i in range(nr):
            d = {'a': i}
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

