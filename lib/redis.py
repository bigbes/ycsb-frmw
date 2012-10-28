#!/usr/bin/env python

from db import DB 
from db import timet, chroot_, cleanup
import shlex
import os
from subprocess import PIPE, STDOUT, Popen
from time import sleep, time
import fileinput

class Redis(DB):
	_exe = "redis-server"
	_cli = "redis-cli"
	_cnf = "redis.conf"
	_log = "redis.log"

	_clean = [".rdb", ".aof", ".log"]

	def __init__(self, _dir):
		self._dir = _dir
		self._run = None
		self.port = "3679"
		if not (os.path.exists(self._dir+'/'+self._exe) and 
				os.path.exists(self._dir+'/'+self._cli)):
			raise Exception('No such file or directory in DB: ' + self._dir)

	def __del__(self):
		self.stop()

	@chroot_
	def init(self):
		cleanup(self._clean)
		print ">>Cleanup Redis"

	def flush_db(self):
		if self._run:
			Popen(shlex.split(self._dir+self._cli_dir+self._cli+" -p "
				+self.port+" \"flushall\"")).wait()
		else:
			print "<<Start Redis, Please"

	def save_snapshot(self):
		if not self._run:
			print "<<Start Redis, Please"
			return -1
		ts = 0; te = 0;
		Popen(shlex.split(self._dir+self._cli_dir+self._cli+" -p "
			+self.port+" \"save\"")).wait()
		fi = fileinput.input(self._dir+self._exe_dir+self._log)
		for line in fi:
			if line.find("Starting DB saving") != -1:
				#print line.split()
				ts = float(line.split()[1])
			elif line.find("DB saved") != -1:
				#print line.split()
				te = float(line.split()[1])
			if ts and te:
				break
		fi.close()
		return te-ts
	
	def load_snapshot(self):
		ts = 0;
		fi = fileinput.input(self._dir+self._exe_dir+self._log)
		for line in fi:
			if line.find("DB loaded") != -1:
				#print line.split()
				if line.find("append") != -1:
					ts = float(line.split()[9])
				else:
					ts = float(line.split()[7])
				break
		fi.close()
		return ts
	
	@chroot_
	def start(self):
		if self._run:
			print "Redis already started"
		print ">>Starting Redis"
		self._run = Popen(shlex.split("./"+self._exe+" "+self._cnf))
		print ">>Redis PID:", self._run.pid

	def stop(self):
		if self._run:
			print ">>Stopping Redis"
			self._run.terminate()
		self._run = None
