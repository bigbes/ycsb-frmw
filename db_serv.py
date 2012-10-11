#!/usr/bin/env python
from pprint import pprint 
from time import sleep
import argparse
import socket

from configobj import ConfigObj



args = []
port = 31897


def parse_cfg():
	global port
	
	config = ConfigObj(args.file)
	
	DB_init = config['DB']
	DBS = config['DBS']

	port = int(config.get('port', port))
	return DB_init, DBS

def check_arguments():
	global args

	parser = argparse.ArgumentParser()
	parser.add_argument('--file', default='_db_serv.cfg', type=str)
	args = parser.parse_args()


def init():
	check_arguments()
	_db, DBS = parse_cfg()
	
	_db_class = {}
	_db_spec  = {}

	# Importing db classes from lib
	__import__("lib", globals(), locals(), [], -1)
	__import__("lib.db", globals(), locals(), ["DB"], -1)
	for i in _db:
		print "importing", i
		#_db_class[i.capitalize()] = getattr(__import__("lib."+i, globals(), locals(), [], -1))()
		_db_class[i.capitalize()] = getattr(__import__("lib."+i.lower(), globals(), locals(), [i.capitalize()], -1), i.capitalize())()

	
	for i, j in DBS.iteritems():
		_db_spec[i] = _db_class[j['db']]
		_db_spec[i].set_dir(j['_d'])
	
	return _db_class, _db_spec

def main(_db_spec):
	def undefined(db):
		return "FAIL UndefinedCommand"
	
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.bind(('', port))
	
	while True:
		sock.listen(1)
		conn, addr = sock.accept()
		data = conn.recv(1024).split()
		pprint (data)
		if len(data) < 2 or len(data) > 2:
			result = "FAIL BadCommand"
		elif not data[1] in _db_spec:
			result = "FAIL WrongDatabase"
		else:
			result = { 		'run'	: (lambda x : "OK" if not _db_spec[x].start() 			else "FAIL CantStart"),
							'stop'	: (lambda x : "OK" if not _db_spec[x].stop() 			else "FAIL CantStop"),
							'init'	: (lambda x : "OK" if not _db_spec[x].init() 			else "FAIL CantInit"),
							'ss'	: (lambda x : "OK" if not _db_spec[x].save_snapshot() 	else "FAIL CantSaveSnap"),
							'ls'	: (lambda x : "OK" if not _db_spec[x].load_snapshot() 	else "FAIL CantLoadSnap"),
							'fdb'	: (lambda x : "OK" if not _db_spec[x].flush() 			else "FAIL CantFlush")
					 }.get(data[0], undefined)(data[1])
		conn.sendall(result)
		conn.close()
		sleep(3)

if __name__ == '__main__':
	_db_class, _db_spec = init()

	main(_db_spec)
