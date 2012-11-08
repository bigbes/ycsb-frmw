#!/usr/bin/env python

from db import DB
from db import timet, chroot_

import shlex
import shutil
import os
import fileinput

from time import sleep, time
from subprocess import PIPE, STDOUT, Popen
from copy import deepcopy
from pprint import pprint

class MongoDB(DB):
	_exe = "mongod"
	_cli = "mongo"
	_cnf = "mongodb.conf"
	_log = "mongodb.log"

	st_args = { 
			'dbpath'  			: './temp',
			'logpath' 			: './mongodb.log',
			'bind_ip' 			: '127.0.0.1',
			'diaglog'			: '3',
			'logappend'			: '',
			'nojournal'			: '',
			'noauth'  			: '',
			'nohttpinterface' 	: '',
			'noprealloc' 		: ''
			}

	def __init__(self, _dir):
		self._dir = _dir
		self._run = None
		self.port = '27017'
		self._args = deepcopy(self.st_args)
		self.add_arg(('port', self.port))
		if not (os.path.exists(self._dir+'/'+self._exe) and 
				os.path.exists(self._dir+'/'+self._cli)):
			raise Exception('No such file or directory in DB: ' + self._dir)

	def set_port(self, port):
		self.port = port
		self.add_arg(('port', self.port))

	def __del__(self):
		self.stop()
	
	@chroot_
	def cleanup(self):
		try:
			shutil.rmtree('temp')
		except OSError:
			pass
		try:
			os.mkdir('temp')
		except OSError:
			pass

	def init(self):
		self.cleanup()
		print ">>Cleanup MongoDB"
	
#add set_port for some reason in other DB
	def flush_db(self):
		if self._run:
			Popen(shlex.split(self._dir+self._cli+"localhost"
				+self.port+"/ycsb --eval \"db.dropDatabase()\"")).wait()
			print ">>Flushing MongoDB"
		else:
			print "<<Start MongoDB, Please"
	
	def	load_snapshot(self):
		print "<<Not Yet Implemented"
		return -1

	def add_arg(self, args):
		self._args[str(args[0])] = str(args[1])


	def save_snapshot(self):
		print "<<Not Yet Implemented"
		return -1

	@chroot_
	def start(self):
		if self._run:
			print "<<MongoDB already started."
		print ">>Starting MongoDB"
		args = shlex.split("./"+self._exe+self.args_to_str())
		self._run = Popen(args)
		print ">>MongoDB PID:", self._run.pid
	
	def stop(self):
		if self._run:
			self._run.terminate()
			print ">>Stopping MongoDB"
		self._run = None

	def args_to_str(self):
		return ''.join(map(lambda x: ' --'+x+' '+self._args[x], self._args))


