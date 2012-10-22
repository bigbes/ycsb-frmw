#!/usr/bin/env python

import argparse
import shlex
import shutil
import json
import os
import pickle
import time
from numpy import var, average
from subprocess import Popen, PIPE
from pprint import pprint
from gnuplot import Plot

from configobj import ConfigObj
from lib.classes import Workload, Answers, DB_client

args = None
lol_dir = 0
ycsb = []

class YCSB:
	def __init__(this):
		this._d="/home/bigbes/src/ycsb-0.1.4"
		this._d_bin = "bin/"
		this._d_wl = "workloads/"
		this._f_cfg = "ycsb.conf"
		this._f_json = "ycsb_export.json"
	def __str__(this):
		return "YCSB(%r)" % (str(this.__dict__))
	__repr__ = __str__

def parse_cfg():
	config = ConfigObj(args.file)
	ops = config['OPS'].keys()	
	_args = config['ARGS']
	wl = []
	for i, j in config['WL'].iteritems():
		t = j['thread']
		wl.append( 
				Workload(
				name = i, 
				wl = j['name'], 
				type = j['type'], 
				params = j['params'],
				threads = xrange(int(t[0]), int(t[1]), int(t[2])) if isinstance(t, (tuple, list)) else int(t),
				args = _args
				) 
		)
	dbs = []
	port = config['NET_DB']['port']
	for i, j in config['NET_DB'].iteritems():
		if isinstance(j, dict):
			temp = DB_client(i, j['host'], port, j['_type'])
			dbs.append(temp)
	ycsb = YCSB()
	for i, j in config['DIR'].iteritems():
		setattr(ycsb, i, j)
	ycsb._f_json = _args['exportfile']
	lol_dir = config['_dir']
	return ops, wl, dbs, ycsb, _args, lol_dir

def check_arguments():
	global args
	parser = argparse.ArgumentParser()
	parser.add_argument('--file', default='_bench.cfg', type=str)
	parser.add_argument('--data', default=None,  type=str)
	args = parser.parse_args()

def __run(wl, thread, db):
	global ycsb
	_data = {}
	def import_stderr(data):
		_data[u'throughput'] = {unicode(k[0]) : float((k[1].replace(',','.'))) for k in data}
	def import_json(data):
		runtime = 0
		for k in xrange(len(data)):
			if data[k][u'measurement'] == u'RunTime(ms)':
				runtime = int((int(data[k][u'value']) - 1) / 500) + 1
			if (data[k][u'measurement'].find(u'Return') != -1 
					and data[k+1][u'measurement'].find(u'Return') == -1):
				if runtime != 0:
					_data['series ' + data[k][u'metric']] = {}
					n = _data['series '+data[k][u'metric']]
					for m in xrange(1, runtime+1):
						if (k+m == len(data) or data[k+m][u'metric'] != data[k][u'metric']):
							break
						n[data[k+m][u'measurement']] = data[k+m][u'value'];
			if data[k][u'measurement'] == "RunTime(ms)":
				_data[u'RunTime'] = data[k][u'value'] / 1000;
			if data[k][u'measurement'] == u'Throughput(ops/sec)':
				_data[data[k][u'metric']+u' Throughput'] = data[k][u'value']
			if data[k][u'measurement'] == u'AverageLatency(us)':
				_data[data[k][u'metric']+u' AvLatency'] = data[k][u'value']
	
	def parse_stderr(string):
		return [(line.split()[0], line.split()[4]) for line in string.split('\n') if line.find("current ops/sec") != -1]
	def parse_json(name):
		return json.loads(("[ " + open(name).read() + " ]").replace("} {", "}, {"))
	
	_prev_root = os.getcwd()
	_new_root = ycsb._d
	os.chdir(_new_root)
	progr = shlex.split("bin/ycsb " + wl.type + " " + db._type 
			+ " -P " + ycsb._d_wl + wl.wl + wl.gen_params() 
			+ " -threads " + str(thread) + " -s " + wl.gen_args())
	print progr
	YCSB = Popen(progr, stdout = PIPE, stderr = PIPE)
	import_stderr(parse_stderr(YCSB.communicate()[1]))
	_json = parse_json(ycsb._f_json)
	pprint(_json, open('1', 'w'))
	import_json(_json)
	os.chdir(_prev_root)

	return _data

def _load_wl(wl, db):
	temp = Workload(
			name = '',
			type = 'load',
			threads = '6',
			wl = 'workloada',
			params = wl.params,
			args = wl.args
		)
	__run(temp, temp.threads, db)

def _run_time(wl, dbs, times = 1):
	ans = Answers()
	for db in dbs:
		db.init()
		db.start()
		if (wl.type == 'run'):
			_load_wl(wl, db)
		for i in xrange(times):
			ans.insert(db.name, wl.threads, __run(wl, wl.threads, db))
		db.stop()
	return ans
	
def _run_thread(wl, dbs, times = 1):
	ans = Answers()
	for db in dbs:
		db.init()
		db.start()
		if (wl.type == 'run'):
			_load_wl(wl, db)
		for thr in wl.threads:
			for i in xrange(times):
				ans.insert(db.name, thr, __run(wl, thr, db))
				if (wl.type == 'load'):
					db.stop()
					db.init()
					db.start()
		db.stop()
	return ans

def plot_latency(wl, Ans, DBS, OPS, _dir):
	os.system('rm -fr "%s"' % _dir)
	os.mkdir(_dir)
	os.chdir(_dir)
	table = { str(i) : { j.name : {} for j in DBS } for i in (wl.threads if isinstance(wl.threads, xrange) else xrange(wl.threads, wl.threads + 1, 1)) }
	for i2 in OPS:
		files = []
		for i1 in DBS:
			for i3 in wl.threads if isinstance(wl.threads, xrange) else xrange(wl.threads, wl.threads + 1, 1):
				ar = Ans.get(i1.name, i3)
				if not i2+" AvLatency" in ar[0].keys():
					continue

				if isinstance(wl.threads, xrange):
					lol = []
					for i in ar:
						print i
						lol.append(i[i2+" AvLatency"])
					table[str(i3)][i1.name][i2] = average(lol)
					#table[str(i3)][i1.name][i2] = average(map(lambda x: x[unicode(i2+" AvLatency")], ar))
				else:
					time = sorted(reduce(lambda x, y: set(x).union(set(y)), map(lambda x: x['series '+i2].keys(), ar)), key=int)
					latency = map(lambda z: average(z), zip(*map(lambda x: map(lambda y: x['series '+i2][y], sorted(x['series '+i2].keys(), key=int)), ar)))
					table[str(i3)][i1.name][i2] = zip(time, latency)
					name = "%(name)s_%(db)s_%(op)s_%(type)%s.data" % 
							{ "name" 	: wl.name,
							  "db" 		: i1.name,
							  "op" 		: i2,
							  "type" 	: "time"}
					fd = open(name ,"w")
					files.append(name)
					for i in table[str(i3)][i1.name][i2]:
						fd.write(str(i[0])+" "+str(i[1]))
		if len(files) != 0:
			

	if isinstance(wl.threads, xrange):
		table1 = { i.name : {} for i in DBS }
		for i2 in OPS:
			files = []
			for i1 in DBS:
				if not table[table.keys()[0]][i1.name].has_key(i2):
					continue
				table1[i1.name][i2] = zip([k for k in wl.threads], map(lambda x: x[i1.name][i2], map(lambda y: table[y], sorted(table.keys(),key=int))))
				fd = open("%(name)s_%(db)s_%(op)s_%(type)s.data" % 
						{ "name" 	: wl.name,
						  "db" 		: i1.name,
						  "op" 		: i2,
						  "type" 	: "thr-range"}, "w")
				for i in table1[i1.name][i2]:
					fd.write(str(i[0])+" "+str(i[1]))
	table = table1
	os.chdir('..')
	return table

def plot_throughput(wl, Ans, DBS, OPS, _dir):
	pass

def save_dump(wl, ans, _str):
	f = open(_str+"_hash_dump_wl", 'w')
	pickle.dump(wl, f)
	open(_str+"_hash_dump", 'w').write(json.dumps(ans._hash))
	pprint(ans._hash, open(_str+"_repr_dump", 'w'))
if __name__ == '__main__':
	check_arguments()
	OPS, WL, DBS, ycsb, ARGS, lol_dir = parse_cfg()
	pprint(DBS)
	for i in WL:
		ans = [] 
		if isinstance(i.threads, xrange):
			ans = _run_thread(i, DBS, 2)
		else:
			ans = _run_time(i, DBS, 2)
		save_dump(i, ans, time.strftime("%Y%m%d_%H%M%S"))
		plot_latency(i, ans, DBS, OPS, lol_dir)
#		plot_threads(wl, ans, DBS, OPS)

	#pprint(ans._hash)
