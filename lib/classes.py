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
        return reduce(lambda x,y:x+y, map(lambda x:" -p {0}={1}".format(x[0],x[1]) ,this.args.items()))

    def gen_params(this):
        return reduce(lambda x,y:x+y, map(lambda x:" -p {0}={1}".format(x[0],x[1]) ,this.params.items()))
    
    def __str__(this):
        return "Workload(%r)" % (str(this.__dict__))

    __repr__ = __str__

class Answers:
    def __init__(this):
        this._hash = {}

    def insert(this, db, thread, data):
        # print str(db)+" "+str(thread)
        if str(db)+" "+str(thread) in this._hash:
            this._hash[str(db)+" "+str(thread)].append(data)
        else:
            this._hash[str(db)+" "+str(thread)] = [data]

    def get(this, db, thread):
        return this._hash[str(db)+" "+str(thread)]

    def __str__(this):
        return str(this._hash)

    def merge(this, ans2):
        for i, j in ans2._hash.iteritems():
            i = i.split()
            for j1 in j:
                this.insert(i[0], i[1], j1)

    def add_op(self, op):
        self.op = self.op + op if hasattr(op, self) else [op]

    def get_ops(self):
        return self.op

    __repr__ = __str__

class DB_client:
    params = {  
            'tarantool' : ' -p tnt.host=%(host)s -p tnt.port=%(port)d ',
            'redis'     : ' -p redis.host=%(host)s -p redis.port=%(port)d ',
            'mongodb'   : ' -p mongodb.url=mongodb://%(host)s:%(port)d -p mongodb.writeConcern=safe'
            }
    def gen_args(self):
        return self.params[self._type] % {
                'host' : self.host,
                'port' : self.db_port
                }

    def __init__(this, name, host, port, db_port, _type):
        this.name = name
        this.host = host
        this.port = int(port)
        this.db_port = int(db_port)
        this._type = _type 

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
            raise Exception(str(answer))
        else:
            answer = 0;
        return str(answer)  
    
    def __str__(this):
        return str("DB_Client " + this.name 
                + " " + this._type + " " 
                + this.host + ":" + str(this.port))

    __repr__ = __str__

    def start(this):
        return this.__send_get__("run "+this.name)

    def stop(this):
        return this.__send_get__("stop "+this.name)

    def init(this):
        return this.__send_get__("init "+this.name)

#   def save_snap(this):
#       answer = this.__send_get__("ss "+this.name)
#       if answer != "OK":
#           pprint ">> " answer;
#           return -1;
#       return 0;
#   
#   def load_snap(this):
#       answer = this.__send_get__("ls "+this.name)
#       if answer != "OK":
#           pprint ">> " answer;
#           return -1;
#       return 0;
#
#   def flush_db(this):
#       answer = this.__send_get__("fdb "+this.name)
#       if answer != "OK":
#           pprint ">> " answer;
#           return -1;
#       return 0;
