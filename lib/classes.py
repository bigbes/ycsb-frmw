import os
import time
import json
import math
import shlex
import shutil
import socket
import pprint
import operator
import subprocess
import collections

import lib.hcpy as HC

def geometric_mean(arr):
    return math.pow(reduce(operator.mul, arr), 1.0/len(arr))

def arithmetic_mean(arr):
    return sum(arr)/len(arr)

def load_databases():
    pass

class Answers:
    def __init__(self):
        self.store = collections.defaultdict(list)
        self.op = []

    def append(self, db, thread, data):
        self.store["%s %s" % (str(db), str(thread))].append(data)

    def get(self, db, thread):
        return self.store[str(db)+" "+str(thread)]

    def __str__(self):
        return str(self.store)

    def merge(self, ans):
        for i, j in ans.store.iteritems():
            self.store[i].extend(j)

    def add_op(self, op):
        self.op.append(op)

    def get_ops(self):
        return self.op

    __repr__ = __str__


class YCSBException(Exception):
    pass

class YCSB(object):
    ycsb_defaults = {
        'bin_path':    'bin',
        'wl_path':     'workloads',
        'ycsb_config': 'ycsb.conf',
        'ycsb_export': 'ycsb_export.json'
    }

    def join_find(self, kwargs, name):
        return os.path.join(self.path, kwargs.get(name, self.ycsb_defaults[name]))

    def __init__(self, path, **kwargs):
        self.path = path
        self.bin_path = self.join_find(kwargs, 'bin_path')
        self.wl_path = self.join_find(kwargs, 'wl_path')
        self.ycsb_config = self.join_find(kwargs, 'ycsb_config')
        self.ycsb_export = self.join_find(kwargs, 'ycsb_export')

    def workload(self, name):
        return os.path.join(self.wl_path, name)

    def run(self, workload, threads, db):
        binary = os.path.join(self.bin_path, 'ycsb')
        cmd  = "%s %s %s " % (binary, workload.wl_type, db.db_type)
        cmd += "-P %s %s " % (self.workload(workload.wl), workload.gen_ycsb_args())
        cmd += "-threads %s -s %s " % (str(threads), db.gen_args())
        cmd  = shlex.split(cmd)
        print ' '.join(cmd)

        proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE,
                stderr = subprocess.PIPE,
                cwd = self.path
        )
        output = proc.communicate()
        outname = '%d_%s_%s_%d' % (
                time.time(),
                db.db_type,
                workload.wl_type,
                threads
        )

        if (proc.returncode):
            raise YCSBException('YCSB exit with code %d: %s', proc.returncode, output[1])

        # we are, currently, running only one type, so we mustn't parse many formats, so..
        # parse_json, then parse_stderr. That's all, folks
        shutil.copyfile(self.ycsb_export, outname + '.json')
        open(outname + '.stderr', 'w').write(output[1])
        retval = self.import_json(self.parse_json())
        assert isinstance(retval, dict)
        retval[u'throughput'] = self.import_stderr(self.parse_stderr(output[1]))

        return retval

    def parse_json(self):
        return json.loads(("[ %s ]" % open(self.ycsb_export, 'r').read()).replace("} {", "}, {"))

    def import_json(self, parsed_data):
        retval = {}
        runtime = 0
        for k, row in enumerate(parsed_data):
            if row[u'measurement'] == u'RunTime(ms)':
                runtime = int((int(row[u'value']) - 1) / 500) + 1
            if row[u'measurement'].find(u'Return') != -1 and \
                    parsed_data[k+1][u'measurement'].find(u'Return') == -1 and \
                    runtime != 0:
                n = retval['series ' + row[u'metric']] = {}
                for m in xrange(1, runtime + 1):
                    if (k + m == len(parsed_data) or
                            parsed_data[k + m][u'metric'] != row[u'metric']):
                        break
                    n[parsed_data[k+m][u'measurement']] = parsed_data[k + m][u'value']
            if row[u'measurement'] == "RunTime(ms)":
                retval[u'RunTime'] = row[u'value'] / 1000;
            if row[u'measurement'] == u'Throughput(ops/sec)':
                retval[row[u'metric'] + u' Throughput'] = row[u'value']
            if row[u'measurement'] == u'AverageLatency(us)':
                retval[row[u'metric'] + u' AvLatency'] = row[u'value']
        return retval

    def parse_stderr(self, stderr):
        stderr = [line for line in stderr.split('\n') if line.find('current ops/sec') != -1]
        getvar = lambda arr, ind: [arr[i] for i in ind]
        return [getvar(line.split(), [2, 6]) for line in stderr]

    def import_stderr(self, parsed_stderr):
        return {unicode(k[0]): float((k[1].replace(',', '.'))) for k in parsed_stderr}

    def __str__(self):
        return "YCSB(%s)" % pprint.pformat(self.__dict__)

    __repr__ = __str__


class Workload(object):
    def __init__(self, wl, wl_type, threads, description, args):
        self.wl = wl
        assert(wl_type in ('load', 'run'))
        self.wl_type = wl_type # 'load' or 'run'
        self.threads = threads
        self.description = description[0]
        self.short_name  = description[1]
        self.args = args
        self.load = DummyWorkload()
        if self.wl_type == 'run':
            self.load = Workload(
                wl=wl, wl_type='load', threads=32,
                description=('', ''), args=args
            )

    def gen_ycsb_args(self):
        return ' '.join(['-p %s=%s' % (key, self.args[key]) for key in self.args])

    def run_once(self, config, thread, db):
        return config['ycsb'].run(self, thread, db)

    def run_time(self, config):
        result = Answers()
        for db in config['databases']:
            db.init()
            db.start()
            self.load.run_once(config, 16, db)

            for i in xrange(config['retries']):
                result.append(db.name, self.threads,
                    self.run_once(config, self.threads, db)
                )
            db.stop()
        return result

    def run_thread(self, config):
        result = Answers()
        for db in config['databases']:
            db.init()
            db.start()
            self.load.run_once(config, 16, db)

            for threads in self.threads:
                for i in xrange(config['retries']):
                    result.append( db.name, threads,
                        self.run_once(config, threads, db)
                    )
                    if (self.wl_type == 'load'):
                        db.stop()
                        db.init()
                        db.start()
            db.stop()
        return result

    def run(self, config):
        ans = None
        if isinstance(self.threads, (xrange, list, tuple)):
            ans = self.run_thread(config)
        else:
            ans = self.run_time(config)
        try:
            os.mkdir(config['options']['output'])
        except OSError as e:
            pass

        prev_dir = os.getcwd()
        os.chdir(config['options']['output'])
        if not isinstance(self.threads, int):
            ch1 = self.gen_hc_files_latency(
                    ans, config['databases'], config['operators']
            )
            ch2 = self.gen_hc_files_throughput(
                    ans, config['databases'], config['operators']
            )

        ch1.append(ch2)
        for j in ch1:
            fd = open(j[0], "w")
            fd.write(str(j[1]))
            fd.close()
        os.chdir(prev_dir)

    def gen_hc_files_latency(self, Ans, DBS, OPS):
        table = {str(i): {j.name: {} for j in DBS} for i in self.threads}
        table1 = {i.name: {} for i in DBS}
        graphs = []
        for op in OPS:
            chart = HC.Chart()
            for db in DBS:
                name  = ' '
                for thr in self.threads:
                    ar = Ans.get(db.name, thr)
                    pprint.pprint(ar)
                    if not op + ' AvLatency' in ar[0]:
                        continue
                    table[str(thr)][db.name][op] = geometric_mean(
                            [el[op + ' AvLatency'] for el in ar]
                    )
                if not op in table[str(self.threads[0])][db.name]:
                    break
                table1[db.name][op] = zip([k for k in self.threads],
                                        map(lambda x: x[db.name][op],
                                            map(lambda y: table[y],
                                                sorted(table.keys(), key=int))))
                _data = []
                for i in table1[db.name][op]:
                    _data.append([i[0], round(i[1], 2)])
                chart.add_series(HC.LineSeries(name=db.description, data=_data))
            if not chart.series:
                continue

            name = "%s_%s_latency.json" % (self.short_name, op)

            _title = 'Latency on %s' % op
            _subtitle = '%d records' % self.args['recordcount']
            if self.wl_type == 'run':
                _subtitle += ' and %d operations' % self.args['operationcount']
            _subtitle += '. Less is better'

            chart.title = HC.TitleConfig(text=_title, x=-20)
            chart.subtitle = HC.SubtitleConfig(text=_subtitle, x=-20)
            chart.xAxis = HC.XAxisConfig(
                    title=HC.TitleConfig(text='Clients'),
                    allowDecimals=False
            )
            chart.yAxis = HC.YAxisConfig(
                    title=HC.TitleConfig(text='Latency(usec)')
            )
            js_tooltip  = "return this.series.name + '<br/>' + "
            js_tooltip += "this.x + ' clients :' + this.y + ' usec'"
            chart.tooltip = HC.TooltipConfig(formatter=js_tooltip)
            chart.credits = HC.CreditsConfig(enabled=False)
            graphs.append((name, chart))
        return graphs

    def gen_hc_files_throughput(self, Ans, DBS, OPS):
        table = {str(i): {j.name: 0 for j in DBS} for i in
                (self.threads if isinstance(self.threads, (xrange, tuple, list)) else [self.threads])}
        table1 = {i.name: 0 for i in DBS}
        chart = HC.Chart()
        for db in DBS:
            for thr in self.threads:
                ar = Ans.get(db.name, thr)
                table[str(thr)][db.name] = geometric_mean(
                        [el['OVERALL Throughput'] for el in ar]
                )
            table1[db.name] = zip([k for k in self.threads],
                                map(lambda x: x[db.name],
                                    map(lambda y: table[y],
                                        sorted(table.keys(), key=int))))
            _data = []
            for i in table1[db.name]:
                _data.append([i[0], round(i[1], 3)])
            chart.add_series(HC.LineSeries(name=db.description, data=_data))
        name = '%s_throughput.json' % self.short_name

        _title = "Throughput"
        _subtitle = '%d records' % self.args['recordcount']
        if self.wl_type == 'run':
            _subtitle += ' and %d operations' % self.args['operationcount']
        _subtitle += '. More is better'

        chart.title = HC.TitleConfig(text=_title, x=-20)
        chart.subtitle = HC.SubtitleConfig(text=_subtitle, x=-20)
        chart.xAxis = HC.XAxisConfig(
                title=HC.TitleConfig(text='Clients'),
                allowDecimals=False
        )
        chart.yAxis = HC.YAxisConfig(
                title=HC.TitleConfig(text='RPS')
        )
        js_tooltip  = "return this.series.name + '<br/>' + "
        js_tooltip += "this.x + ' clients :' + this.y + ' usec'"
        chart.tooltip = HC.TooltipConfig(formatter=js_tooltip)
        chart.credits = HC.CreditsConfig(enabled=False)
        return (name, chart)

    def __str__(self):
        return "Workload(%s)" % pprint.pformat(self.__dict__)

    __repr__ = __str__

class DummyWorkload(Workload):
    def __init__(self):
        pass
    def run_once(self, config, thread, db):
        pass

class DBClientException(Exception):
    pass

class DBClient(object):
    params = {
        'tarantool': collections.OrderedDict([
            ('tnt.host', '%s'),
            ('tnt.port', '%d'),
        ]),
        'redis': collections.OrderedDict([
            ('redis.host', '%s'),
            ('redis.port', '%d'),
        ]),
        'mongodb': collections.OrderedDict([
            ('mongodb.url', 'mongodb://%s:%d')
#            ('mongodb.writeConcern', 'safe')
        ])
    }

    def __init__(self, name, host, port, db_port, db_type, description):
        self.name    = name
        self.host    = host
        self.port    = int(port)
        self.db_port = int(db_port)
        self.db_type = db_type
        self.description = description
        self.started = False

    def conn_send_recv(self, cmd):
        retval = -1
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error, msg:
            raise DBClientException(msg)
        try:
            sock.connect((self.host, self.port))
            sock.sendall(cmd)
            retval = sock.recv(1024)
        except socket.error, msg:
            raise DBClientException(msg)
        finally:
            sock.close()
        retval = 0 if retval == 'OK' else -1
        return retval

    def start(self):
        retval = self.conn_send_recv("run %s" % self.name)
        if (retval == 0):
            self.started = True

    def stop(self):
        return self.conn_send_recv("stop %s" % self.name)

    def init(self):
        return self.conn_send_recv("init %s" % self.name)

    def __str__(self):
        return str("DBClient(%s - %s:%d)" % (self.name, self.host, self.port))

    def __del__(self):
        if self.started:
            self.stop()

    __repr__ = __str__

    def gen_args(self):
        arr = self.params[self.db_type]
        arr = ['-p %s=%s' % (k, v) for k, v in arr.iteritems()]
        retval = ' '.join(arr) % (self.host, self.db_port)
        return retval
