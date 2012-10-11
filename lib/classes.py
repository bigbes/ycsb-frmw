#!/usr/bin/env python
import socket

from pprint import pprint

port = 31897

class Workload:
	def __init__(this, name, type, wl, threads, params, args):
		this.name = name
		this.type = type # load, run
		this.threads = threads # xargs or int
		this.wl = wl # workload(a,b,c,d,e,f)
		this.params = params # 
		this.args = args

	def gen_args(this):
		reduce(lambda x,y:x+y, map(lambda x:" -p {0}={1}".format(x[0],x[1]) ,this.args.items()))

	def gen_params(this):
		reduce(lambda x,y:x+y, map(lambda x:" -p {0}={1}".format(x[0],x[1]) ,this.params.items()))
	
	def __str__(this):
		return "Workload(%r)" % (str(this.__dict__))

	__repr__ = __str__

class Answers:
	def __init__(this):
		this._hash = {}

	def insert(this, thread, data):
		if thread in this._hash:
			this._hash[thread].append(data)
		else:
			this._hash[thread] = [data]
	
	def get(this, thread):
		return this._hash[thread]

	def __str__(this):
		return str(this._hash)

	__repr__ = __str__

class DB_client:
	def __init__(this, name, host):
		this.name = name
		this.host = host
		this.port = int(port)

	def set_port(this, _port):
		this.port = int(_port)
	
	def __send_get__(this, string):
		answer = -1
		try:
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		except socket.error, msg:
			pprint(msg)
		try:
			sock.connect((this.host, this.port))
			sock.sendall(string)
			answer = sock.recv(1024)
		except socket.error, msg:
			pprint(msg)
		finally:
			sock.close()
		if answer != "OK":
			pprint(">> " + str(answer))
		else:
			answer = 0;
		return str(answer)
		
	
	def __str__(this):
		return str("DB_Client " + this.name + 
				" " + this.host + ":" + str(this.port))

	__repr__ = __str__

	def start(this):
		return this.__send_get__("run "+this.name)

	def stop(this):
		return this.__send_get__("stop "+this.name)

	def init(this):
		return this.__send_get__("init "+this.name)

#	def save_snap(this):
#		answer = this.__send_get__("ss "+this.name)
#		if answer != "OK":
#			pprint ">> " answer;
#			return -1;
#		return 0;
#	
#	def load_snap(this):
#		answer = this.__send_get__("ls "+this.name)
#		if answer != "OK":
#			pprint ">> " answer;
#			return -1;
#		return 0;
#
#	def flush_db(this):
#		answer = this.__send_get__("fdb "+this.name)
#		if answer != "OK":
#			pprint ">> " answer;
#			return -1;
#		return 0;
