#!/usr/bin/env python

from db import DB 
from db import timet, chroot_, cleanup
import shlex
import os
from subprocess import PIPE, STDOUT, Popen
from time import sleep, time
import fileinput

class Tarantool(DB):
	_exe = "tarantool_box"
	_cli = "tarantool"
	_cnf = "tarantool.cfg"
	_log = "tarantool.log"

	_clean = [".snap", ".xlog", ".log"]

	def __init__(self, _dir):
		self._dir = _dir
		self._run = None
		self.port = "33013"
		if not (os.path.exists(self._dir+'/'+self._exe)):
			raise Exception('No such file or directory in DB: ' + self._dir)

	def __del__(self):
		self.stop()
	
	@chroot_
	def init(self):
		cleanup(self._clean)
		Popen(shlex.split("./"+self._exe+" --init-storage"), stdout=PIPE, stderr=PIPE).wait()
		print ">>Cleanup Tarantool"

	def flush_db(self):
		if self._run:
			Popen(shlex.split(self._dir+self._cli+" -p "
				+self.port+" \"lua box.space[0]:truncate()\"")).wait()
		else:
			print "<<Start Tarantool, Please"
	
	@timet
	def save_snapshot(self):
		if self._run:
			Popen(shlex.split(self._dir+self._cli+" -p "
				+self.port+" \"save snapshot\"")).wait()
		else:
			print "<<Start Tarantool, Please"

	def load_snapshot(self):
		ts = 0; te = 0;
		fi = fileinput.input(self._dir+self._log)
		for line in fi:
			if line.find("recovery start") != -1:
				ts = float(line.split()[0])
			elif line.find("I am primary") != -1:
				te = float(line.split()[0])
			if ts and te:
				break
		fi.close()
		return te-ts
	
	@chroot_
	def start(self):
		if self._run:
			return
		print ">>Starting Tarantool"
		self._run = Popen(shlex.split("./"+self._exe))
		print ">>Tarantool PID:", self._run.pid

	def stop(self):
		if self._run:
			self._run.terminate()
			print ">>Stopping Tarantool"
		self._run = None
