#!/usr/bin/env python

import os
import time

def chroot_(func):
	def new_func(self, *args, **kw):
		_prev_root = os.getcwd()
		_new_root = self._dir
		os.chdir(_new_root)
		result = func(self, *args, **kw)
		os.chdir(_prev_root)
		return result
	return new_func

def timet(func):
	def new_func(self, *args, **kw):
		ts = time.time()
		func(self, *args, **kw)
		te = time.time()
		return te-ts
	return new_func

def cleanup(_clean):
	for i in os.listdir("."):
		for j in _clean:
			if i.find(j) != -1:
				os.remove(i)

class DB:
	_dir = ""
	_exe = ""
	_cli = ""
	_cnf = ""
	_log = "" 

	_host = "localhost"
	_port = ""

	_run = None
	_clean = []

	def set_host(this, host, port = None):
		this._host = host
		if port != None :
			this._port = int(_port)

	def set_dir(this, _dir):
		this._dir = _dir
	
	def set_port(self, port):
		self.port = int(port)
	
	def init():
		pass
	
	def flush_db():
		pass

	def save_snapshot():
		pass

	def load_snapshot():
		pass

	def start():
		pass

	def stop():
		pass
