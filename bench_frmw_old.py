#!/usr/bin/env python

import subprocess
import sys
import argparse
import shlex
import shutil
import json
import os
import pickle
import time
import csv
from subprocess import Popen, PIPE
from pprint import pprint

from configobj import ConfigObj
from lib.classes import Workload, Answers, DB_client
from lib.gnuplot import Plot

import lib.hcpy as HC

def average(_list):
    return sum(_list) / len(_list)


args = None
trials = 2
OPS, WL, DBS, ycsb, ARGS, out_dir = (None, None, None, None, None, None)


class YCSB:
    def __init__(this):
        this._d = "/home/bigbes/src/ycsb-0.1.4"
        this._d_bin = "bin/"
        this._d_wl = "workloads/"
        this._f_cfg = "ycsb.conf"
        this._f_json = "ycsb_export.json"

    def __str__(this):
        return "YCSB(%r)" % (str(this.__dict__))

    __repr__ = __str__


class __Answer:
    def __init__(self, wl):
        self.wl = wl
        self.base = []

    def add_file(self, filename, db, op):
        self.base.append((filename, db, op))


    def __str__(this):
        return "__Answer(%r)" % (str(this.__dict__))

    __repr__ = __str__


def parse_cfg():
    config = ConfigObj(args.file)
    ops = config['OPS'].keys()
    _args = config['ARGS']
    wl = []
    for i, j in config['WL'].iteritems():
        t = j['thread']
        if isinstance(t, (tuple, list)):
            if len(t) == 3:
                t = xrange(int(t[0]), int(t[1]), int(t[2]))
            else:
                t = [int(x) for x in t]
        else:
            t = int(t)
        wl.append(
            Workload(
                name=i,
                wl=j['name'],
                type=j['type'],
                params=j['params'],
                threads=t,
                args=_args
            )
        )
    dbs = []
    for i, j in config['NET_DB'].iteritems():
        if isinstance(j, dict):
            try:
                port = j['serv_port']
            except KeyError:
                port = 31897
            db_port = j['db_port']
            temp = DB_client(i, j['host'], port, db_port, j['_type'])
            dbs.append(temp)
    ycsb = YCSB()
    for i, j in config['DIR'].iteritems():
        setattr(ycsb, i, j)
    ycsb._f_json = _args['exportfile']
    out_dir = config['_dir']
    return ops, wl, dbs, ycsb, _args, out_dir


def check_arguments():
    global args
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', default='_bench.cfg', type=str)
    parser.add_argument('--data', default=None, type=str)
    args = parser.parse_args()


def __run(wl, thread, db):
    global ycsb
    _data = {}

    def import_stderr(data):
        _data[u'throughput'] = {unicode(k[0]): float((k[1].replace(',', '.'))) for k in data}

    def import_json(data):
        runtime = 0
        for k in xrange(len(data)):
            if data[k][u'measurement'] == u'RunTime(ms)':
                runtime = int((int(data[k][u'value']) - 1) / 500) + 1
            if (data[k][u'measurement'].find(u'Return') != -1
                and data[k + 1][u'measurement'].find(u'Return') == -1):
                if runtime != 0:
                    _data['series ' + data[k][u'metric']] = {}
                    n = _data['series ' + data[k][u'metric']]
                    for m in xrange(1, runtime + 1):
                        if (k + m == len(data) or data[k + m][u'metric'] != data[k][u'metric']):
                            break
                        n[data[k + m][u'measurement']] = data[k + m][u'value'];
            if data[k][u'measurement'] == "RunTime(ms)":
                _data[u'RunTime'] = data[k][u'value'] / 1000;
            if data[k][u'measurement'] == u'Throughput(ops/sec)':
                _data[data[k][u'metric'] + u' Throughput'] = data[k][u'value']
            if data[k][u'measurement'] == u'AverageLatency(us)':
                _data[data[k][u'metric'] + u' AvLatency'] = data[k][u'value']

    def parse_stderr(string):
        return [(line.split()[0], line.split()[4]) for line in string.split('\n') if line.find("current ops/sec") != -1]

    def parse_json(name):
        return json.loads(("[ " + open(name).read() + " ]").replace("} {", "}, {"))

    _prev_root = os.getcwd()
    _new_root = ycsb._d
    os.chdir(_new_root)
    progr = shlex.split("bin/ycsb " + wl.type + " " + db._type
                        + " -P " + ycsb._d_wl + wl.wl + wl.gen_params()
                        + " -threads " + str(thread) + " -s " + wl.gen_args() + db.gen_args())

    YCSB = Popen(progr, stdout=PIPE, stderr=PIPE)

    _stderr = YCSB.communicate()

    try:
        _json = parse_json(ycsb._f_json)
        __stderr = parse_stderr(_stderr[1])
        import_json(_json)
        import_stderr(__stderr)
    except:
        with open("1", 'a') as _file:
            pprint(_json, _file)
            pprint(_stderr[0], _file)
            pprint(_stderr[1], _file)
        raise

    os.chdir(_prev_root)

    return _data


def _load_wl(wl, db):
    temp = Workload(
        name='',
        type='load',
        threads='15',
        wl=wl.wl,
        params=wl.params,
        args=wl.args
    )
    __run(temp, temp.threads, db)


def _print(str):
    sys.stdout.write(str)
    sys.stdout.flush()


def _run_time(wl, dbs, times=1):
    ans = Answers()
    for db in dbs:
        db.init()
        db.start()
        _print(db.name + ': [')
        if (wl.type == 'run'):
            _print('_')
            _load_wl(wl, db)

        for i in xrange(times):
            _print('#')
            ans.insert(db.name, wl.threads, __run(wl, wl.threads, db))
        _print(']\n')
        db.stop()
    return ans


def _run_thread(wl, dbs, times=1):
    ans = Answers()
    for db in dbs:
        db.init()
        db.start()
        _print(db.name + ': [')
        if (wl.type == 'run'):
            _print('_')
            _load_wl(wl, db)

        for thr in wl.threads:
            for i in xrange(times):
                _print('#')
                ans.insert(db.name, thr, __run(wl, thr, db))
                if (wl.type == 'load'):
                    db.stop()
                    db.init()
                    db.start()
        _print(']\n')
        db.stop()
    return ans


def plot_latency(wl, Ans, DBS, OPS):
    table = {str(i): {j.name: {} for j in DBS} for i in
             (wl.threads if isinstance(wl.threads, (xrange, tuple, list)) else [wl.threads])}
    table1 = {i.name: {} for i in DBS}
    local = __Answer(wl)
    for i2 in OPS:

        for i1 in DBS:
            name = ' '
            for i3 in (wl.threads if isinstance(wl.threads, (xrange, tuple, list)) else [wl.threads]):
                ar = Ans.get(i1.name, i3)
                if not i2 + " AvLatency" in ar[0].keys():
                    continue
                if isinstance(wl.threads, (xrange, list, tuple)) and len(wl.threads) != 1:
                    table[str(i3)][i1.name][i2] = average(map(lambda x: x[i2 + " AvLatency"], ar))
                else:
                    time = average(map(lambda x: x[u'RunTime'], ar))
                    latency = average(map(lambda x: x[unicode(i2) + u' AvLatency'], ar))
                    print wl.name + " " + str(i2) + " " + i1.name + " " + str(time) + " " + str(latency)
            if isinstance(wl.threads, (list, tuple, xrange)) and len(wl.threads) != 1:
                if not i2 in table[str(wl.threads[0])][i1.name]:
                    continue
                table1[i1.name][i2] = zip([k for k in wl.threads], map(lambda x: x[i1.name][i2],
                                                                       map(lambda y: table[y],
                                                                           sorted(table.keys(), key=int))))
                name = "%(name)s_%(db)s_%(op)s_thr-range.data" % {
                "name": wl.name,
                "db": i1.name,
                "op": i2
                }
                # fd = open(name, "w")
                # for i in table1[i1.name][i2]:
                #     fd.write(str(i[0]) + " " + str(i[1]) + "\n")
                # local.add_file(name, i1.name, i2)
                # fd.close()
    return (table1 if isinstance(wl.threads, (xrange, list, tuple)) else table), local


def plot_throughput(wl, Ans, DBS, OPS):
    table = {str(i): {j.name: 0 for j in DBS} for i in
             (wl.threads if isinstance(wl.threads, (xrange, tuple, list)) else [wl.threads])}
    table1 = {i.name: 0 for i in DBS}
    local = __Answer(wl)
    for i1 in DBS:
        name = ' '
        for i2 in (wl.threads if isinstance(wl.threads, (xrange, list, tuple)) else [wl.threads]):
            ar = Ans.get(i1.name, i2)
            if isinstance(wl.threads, (xrange, list, tuple)) and len(wl.threads) != 1:
                table[str(i2)][i1.name] = average(map(lambda x: x["OVERALL Throughput"], ar))
            else:
                time = average(map(lambda x: x[u'RunTime'], ar))
                throughput = average(map(lambda x: x[u'OVERALL Throughput'], ar))
                print wl.name + " " + i1.name + " " + str(time) + " " + str(throughput)
        if isinstance(wl.threads, (xrange, list, tuple)):
            table1[i1.name] = zip([k for k in wl.threads], map(lambda x: x[i1.name],
                                                               map(lambda y: table[y], sorted(table.keys(), key=int))))
            name = "%(name)s_%(db)s_throughput_thr-range.data" % {
            "name": wl.name,
            "db": i1.name,
            }
            # fd = open(name, "w")
            # for i in table1[i1.name]:
            #     fd.write(str(i[0]) + " " + str(i[1]) + "\n")
            # local.add_file(name, i1.name, i2)
            # fd.close()
    return (table1 if isinstance(wl.threads, (xrange, list, tuple)) else table), local


def save_dump(wl, ans, _str):
    f = open(_str + "_repr_dump", 'w')
    pprint(ans._hash, f)
    f.close()


def gen_gnuplot_files_latency(_ans, OPS):
    wl = _ans.wl
    for i2 in OPS:
        t_ans = filter(lambda x: x[2] == i2, _ans.base)
        if len(t_ans) == 0:
            continue
        if wl.type == 'run':
            name = "%(name)s_%(op)s_%(type)s" % {
            "name": (wl.wl[-1]).upper(),
            "op": i2,
            "type": "latency"
            }
        else:
            name = "LOAD_INSERT_latency"
        title = ("Loading " if wl.type == 'load' else "Running ")
        title += wl.wl + " "
        title += "on " + i2 + " test. "
        title += (str(wl.threads) + ' clients ' if not isinstance(wl.threads, xrange) else ' ')
        title += 'with ' + ((str(wl.params['operationcount']) + ' operations') if wl.type == 'run' else (
        str(wl.params['recordcount']) + ' records'))
        plotfile = Plot(name + '.plot', name, _format='svg').set_title(title,
                                                                       'Clients' if isinstance(wl.threads, (
                                                                       xrange, tuple, list)) else 'Time(sec)',
                                                                       'Latency(usec)',
                                                                       wl.threads if isinstance(wl.threads,
                                                                                                (tuple, list)) else None
        )
        for i2 in t_ans:
            plotfile.add_data(i2[0], i2[1])
        plotfile.gen_file()


def gen_gnuplot_files_throughput(_ans, OPS):
    wl = _ans.wl
    if wl.type == 'run':
        name = "%(name)s_%(type)s" % {
        "name": (wl.wl[-1]).upper(),
        "type": "throughput"
        }
    else:
        name = "LOAD_throughput"
    title = ("Loading " if wl.type == 'load' else "Running ")
    title += wl.wl + " "
    title += (str(wl.threads) + ' clients ' if not isinstance(wl.threads, xrange) else ' ')
    title += 'with ' + ((str(wl.params['operationcount']) + ' operations') if wl.type == 'run' else (
    str(wl.params['recordcount']) + ' records'))
    plotfile = Plot(name + '.plot', name, _format='svg').set_title(title,
                                                                   'Clients' if isinstance(wl.threads, (
                                                                   xrange, tuple, list)) else 'Time(sec)',
                                                                   'RPS',
                                                                   wl.threads if isinstance(wl.threads,
                                                                                            (tuple, list)) else None
    )
    for i in _ans.base:
        plotfile.add_data(i[0], i[1])
    plotfile.gen_file()


def new_db_name(name):
    DB = [  ('rds', 'Redis'),
            ('mongodb', 'MongoDB'),
            ('memcached', 'Memcached')
    ]
    if 'tnt' in name:
        if 'hash' in name:
            name = 'Tarantool with HASH index'
        elif 'tree' in name:
            name = 'Tarantool with TREE index'
    else:
        name = name.replace('_', ' ')
        for i in DB:
            name = name.replace(i[0], i[1])
    return name


def new_wl_name(name):
    WLS = [
        ('workloada', 'Workload A', 'A'),
        ('workloadb', 'Workload B', 'B'),
        ('workloadc', 'Workload C', 'C'),
        ('workloadd', 'Workload D', 'D'),
        ('workloade', 'Workload E', 'E'),
        ('workloadf', 'Workload F', 'F')
    ]
    for i in WLS:
        name = name.replace(i[0], i[1])
    return name


def gen_hc_files_latency(workload, Ans, databases, operations):
    DBS = databases
    OPS = operations
    wl = workload
    table = {str(i): {j.name: {} for j in DBS} for i in wl.threads}
    table1 = {i.name: {} for i in DBS}
    graphs = []
    for i2 in OPS:
        chart = HC.Chart()
        for i1 in DBS:
            name = ' '
            for i3 in wl.threads:
                ar = Ans.get(i1.name, i3)
                if not i2 + ' AvLatency' in ar[0].keys():
                    continue
                table[str(i3)][i1.name][i2] = average(map(lambda x: x[i2 + " AvLatency"], ar))
            if not i2 in table[str(wl.threads[0])][i1.name]:
                break
            table1[i1.name][i2] = zip([k for k in wl.threads], map(lambda x: x[i1.name][i2],
                                                                   map(lambda y: table[y],
                                                                       sorted(table.keys(), key=int))))
            _data = []
            for i in table1[i1.name][i2]:
                _data.append([i[0], round(i[1], 2)])
            chart.add_series(HC.LineSeries(name=new_db_name(i1.name), data=_data))
        if not chart.series:
            continue

        if wl.type == 'run':
            name = "%(name)s_%(op)s_%(type)s" % {
                "name": (wl.wl[-1]).upper(),
                "op": i2,
                "type": "latency"
            }
        else:
            name = "LOAD_INSERT_latency"

        _title = 'Latency on ' + i2
        #_title += (str(wl.threads) + ' clients ' if not isinstance(wl.threads, xrange) else ' ')
        _subtitle =  (str(wl.params['recordcount']) + ' records') + ((' and ' + str(wl.params['operationcount']) +
                                                                      ' operations. ') if wl.type == 'run' else '. ') + 'Less is better'


        chart.title = HC.TitleConfig(text=_title, x=-20)
        chart.subtitle = HC.SubtitleConfig(text=_subtitle, x=-20)
        chart.xAxis = HC.XAxisConfig(title=HC.TitleConfig(text='Clients'), allowDecimals=False)
        chart.yAxis = HC.YAxisConfig(title=HC.TitleConfig(text='Latency(usec)'))
        chart.tooltip = HC.TooltipConfig(formatter='return this.series.name + \'<br/>\' + this.x + \' clients : \' + this.y + \' usec\'')
        chart.credits = HC.CreditsConfig(enabled=False)
        graphs.append((name, chart))
    return table1, graphs


def gen_hc_files_throughput(workload, Ans, databases, operations):
    DBS = databases
    OPS = operations
    wl = workload
    table = {str(i): {j.name: 0 for j in DBS} for i in
             (wl.threads if isinstance(wl.threads, (xrange, tuple, list)) else [wl.threads])}
    table1 = {i.name: 0 for i in DBS}
    chart = HC.Chart()
    for i1 in DBS:
        for i2 in wl.threads:
            ar = Ans.get(i1.name, i2)
            table[str(i2)][i1.name] = average(map(lambda x: x["OVERALL Throughput"], ar))
        table1[i1.name] = zip([k for k in wl.threads], map(lambda x: x[i1.name],
                                                               map(lambda y: table[y], sorted(table.keys(), key=int))))
        _data = []
        for i in table1[i1.name]:
            _data.append([i[0], round(i[1], 3)])
        chart.add_series(HC.LineSeries(name=new_db_name(i1.name), data=_data))
    if wl.type == 'run':
        name = "%(name)s_%(type)s" % {
            "name": (wl.wl[-1]).upper(),
            "type": "throughput"
        }
    else:
        name = "LOAD_throughput"

    _title = "Throughput"
    _subtitle =  (str(wl.params['recordcount']) + ' records') + ((' and ' + str(wl.params['operationcount']) +
                                                                  ' operations. ') if wl.type == 'run' else '. ') + 'More is Better.'

    chart.title = HC.TitleConfig(text=_title, x=-20)
    chart.subtitle = HC.SubtitleConfig(text=_subtitle, x=-20)
    chart.xAxis = HC.XAxisConfig(title=HC.TitleConfig(text='Clients'), allowDecimals=False)
    chart.yAxis = HC.YAxisConfig(title=HC.TitleConfig(text='RPS'))
    chart.tooltip = HC.TooltipConfig(formatter='return this.series.name + \'<br/>\' + this.x + \' clients : \' + this.y + \' RPS\'')
    chart.credits = HC.CreditsConfig(enabled=False)
    return table1, (name, chart)

def gen_csv_files(tablelat, tablethr, OPS, wl):
    xval = map(lambda a: a[0], next(next(tablelat.itervalues()).itervalues()))
    with open('ycsb.csv', 'a') as csvfile:
        csvwrite = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL, delimiter=',')
        csvwrite.writerow([wl.wl, wl.type])
        csvwrite.writerow(['clients(xaxis)'] + xval)
        for j in OPS:
            if j not in next(tablelat.itervalues()):
                break
            csvwrite.writerow(['(yaxis)Latency ' + j])
            for k, v in tablelat.items():
                csvwrite.writerow([k] + map(lambda a: a[1], v[j]))
        csvwrite.writerow(['(yaxis)Throughput'])
        for k, v in tablethr.items():
            csvwrite.writerow([k] + map(lambda a: a[1], v))

if __name__ == '__main__':
    check_arguments()
    OPS, WL, DBS, ycsb, ARGS, out_dir = parse_cfg()
    pprint(DBS)
    for i in WL:
        print i
        sys.stdout.write('Workload %(wl)s, %(tests)s\n' % {
        'wl': i.type + ' ' + i.wl,
        'tests': (len(i.threads) if isinstance(i.threads, xrange) else 1) * trials
        })
        ans = []
        if isinstance(i.threads, (xrange, list, tuple)):
            ans = _run_thread(i, DBS, trials)
        else:
            ans = _run_time(i, DBS, trials)
        save_dump(i, ans, time.strftime("%Y%m%d_%H%M%S"))
        try:
            os.mkdir(out_dir)
        except OSError:
            pass

        os.chdir(out_dir)

        print ans

        if not isinstance(i.threads, int):
            lol_1, ch1 = gen_hc_files_latency(i, ans, DBS, OPS)
            lol_2, ch2 = gen_hc_files_throughput(i, ans, DBS, OPS)

        ch1.append(ch2)
        for j in ch1:
            fd = open(j[0], "w")
            fd.write(str(j[1]))
            fd.close()
        # lol_1, _ans = plot_latency(i, ans, DBS, OPS)
        # if not isinstance(i.threads, int):
        #     gen_gnuplot_files_latency(_ans, OPS)
        #
        # lol_2, _ans = plot_throughput(i, ans, DBS, OPS)
        # if not isinstance(i.threads, int):
        #     gen_gnuplot_files_throughput(_ans, OPS)

        #gen_csv_files(lol_1, lol_2, OPS, i)

        os.chdir('..')
