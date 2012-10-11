#!/usr/bin/env python

from subprocess import Popen, PIPE

args = None

class YCSB:
	def __init__(this):
		this._d="/home/bigbes/src/ycsb-0.1.4/"
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
			temp = DB_client(i, j['host'])
			temp.set_port(port)
			dbs.append(temp)
	ycsb = YCSB()
	for i, j in config['DIR'].iteritems():
		setattr(ycsb, i, j)
	ycsb._f_json = _args['exportfile']
	return ops, wl, dbs, ycsb, _args

def check_arguments():
	global args
	parser = argparse.ArgumentParser()
	parser.add_argument('--file', default='_bench.cfg', type=str)
	parser.add_argument('--data', default=None,  type=str)
	args = parser.parse_args()

def __run(wl, db, thread):
	def import_stderr(data, i, j):
		_hash[i][j][u'throughput'] = {(unicode(k[0]), float((k[1].replace(',','.'))) for k in data}
	def import_json(data, i, j):
		runtime = 0
		for k in xrange(len(data)):
			if data[k][u'measurement'] == u'RunTime(ms)':
				runtime = int((int(data[k][u'value']) - 1) / 2000) + 1
			if (data[k][u'measurement'].find(u'Return') != -1 
					and data[k+1][u'measurement'].find(u'Return') == -1):
				if runtime != 0:
					_hash[i][j]['series ' + data[k][u'metric']] = {}
					n = _hash[i][j]['series '+data[k][u'metric']]
					for m in xrange(1, runtime+1):
						if (k+m == len(data) or data[k+m][u'metric'] != data[k][u'metric']):
							break
						n[data[k+m][u'measurement']] = data[k+m][u'value'];
			if data[k][u'measurement'] == "RunTime(ms)":
				_hash[i][j][u'RunTime'] = data[k][u'value'] / 1000;
			if data[k][u'measurement'] == u'Throughput(ops/sec)':
				_hash[i][j][data[k][u'metric']+u' Throughput'] = data[k][u'value']
			if data[k][u'measurement'] == u'AverageLatency(us)':
				_hash[i][j][data[k][u'metric']+u' AvLatency'] = data[k][u'value']
	
	def parse_stderr(string):
		return [(line.split()[0], line.split()[4]) for line in string.split('\n') if line.find("current ops/sec") != -1]
	def parse_json(name):
		return json.loads(("[ " + open(name).read() + " ]").replace("} {", "}, {"))

	open(_file_ycsb_json, 'w').close()
	progr = shlex.split(ycsb._d + ycsb._d_bin + "ycsb " + wl.type + " " + db
		+ " -P " + ycsb._d + ycsb._d_wl + wl.workload + " -P " + ycsb._f_cfg
		+ wl.gen_params() + " -threads" + str(thread) + " -s " + wl.gen_args())
	print progr

	ycsb = Popen(progr, stdout = PIPE, stderr = PIPE)
	import_stderr(parse_stderr(ycsb.communicate()[1]), thread, db)
	import_json(parse_json(ycsb._f_json), thread, db)	

def _load_wl(wl, db):
	temp = Workload(
			name = '',
			type = 'load',
			threads = '6'
			wl = 'workloada',
			params = wl.params,
			args = wl.args
		)
	__run(wl, db, temp.threads)

def _run_time(wl, dbs):
	for db in dbs:
		db.init()
		db.start()
		if (wl.type == 'run'):
			_load_wl(wl, db.name)
		__run(wl, wl.threads, db)
		db.stop()
	
def _run_thread(wl, dbs):
	for db in dbs:
		db.init()
		db.start()
		if (wl.type == 'run'):
			_load_wl(wl, db.name)
		for thr in wl.threads:
			__run(wl, wl.threads, db)
			if (wl.type == 'load'):
				db.stop()
				db.init()
				db.start()
		db.stop()	

def plot_la_threads(wl, Ans):
	pass

def plot_th_threads(wl, Ans):
	pass

def plot_la_time(wl, Ans):
	pass

def plot_th_time(wl, Ans):
	pass

if __name__ == '__main__':
	check_arguments()
	OPS, WL, DBS, YCSB = parse_cfg()
	Ans = Answer()
	for i in WL:
		if isinstance(i.threads, xrange):
			_run_thread(i, DBS, Ans)
			plot_la_threads()
			plot_th_threads()
		else:
			_run_time(i, DBS, Ans)
			plot_la_time()
			plot_th_time()


