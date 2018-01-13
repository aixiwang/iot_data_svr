# -*- coding=gbk -*- 
#-----------------------------------------------------------
# Copyright (c) 2015 by Aixi Wang <aixi.wang@hotmail.com>
#-----------------------------------------------------------
import site
import logging, sys, os, random, signal, time
from SocketServer import ThreadingTCPServer
import leveldb
import thread

import logging, socket
from SocketServer import BaseRequestHandler
from functools import partial
from json import JSONDecoder, JSONEncoder
#from rpc_handler import CommRpcHandler

#------------------------------------------
# global variables
#------------------------------------------
MAIN_VERSION = 'v0.15'
LISTEN_ADDR = '127.0.0.1'
LISTEN_PORT = 7777
REQ_MAX_LEN = 1024*1024

rpc_auth_key = ''
rpc_handlers = {}
requests = []
db_handlers = {}

LOG_FILENAME = 'iot_data_svr.log.txt'

#------------------------------------------
# common utils routines
#------------------------------------------
def readfile(filename):
    f = file(filename,'rb')
    fs = f.read()
    f.close()
    return fs

def writefile(filename,content):
    f = file(filename,'wb')
    fs = f.write(content)
    f.close()
    return
    
def has_file(filename):
    if os.path.exists(filename):
        return True
    else:
        return False
        
def remove_file(filename):
    if has_file(filename):
        os.remove(filename)

#-------------------
# log_dump
#-------------------
def log_dump(filename,content):
    if os.name == "nt":
        fpath = '.\\' + filename
    else:
        fpath = './' + filename
    
    f = file(fpath,'ab')
    t_s = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    fs = f.write(t_s + '->' + str(content) + '\r\n')
    f.close()
    return
    
#-------------------------------------------------
# CommRpcHandler
#-------------------------------------------------        
import logging, random, time

class CommRpcHandler:
    def __init__(self,auth_key):
        self.rpc_auth_key = auth_key
        pass

    def shutdown(self):
        pass

    def handle_json(self, request):
        try:
            key = request["key"]
            cmd = request["cmd"]
            if cmd == "ping":
                return self.handle_ping(request)
            if cmd == "exit":
                return self.handle_exit(request)        
            if key != self.rpc_auth_key:
                return {'code' : -1, 'data' : 'invalid key'}            
            elif cmd == 'changekey':
                return self.handle_changekey(request)
            elif cmd == 'upgrade':
                return self.handle_upgrade(request)            
            else:
                print 'unsupported command!'
                return {'code' : -1, 'data' : 'un-supported command: '+str(cmd)}
        except:
            print 'CommRpcHandler exception'
            return {'code' : -1, 'data' : 'CommRpcHandler exception!'}            
    def handle_ping(self, request):
        data = 'pong'
        return {'code' : 0, 'data' : data}

    def handle_exit(self, request):    
        print 'exit directly!'
        try:
            sys.exit(-1)
            while (1):
                print 'exiting...'
        except:
            pass
            
    def handle_changekey(self,request):
        global rpc_auth_key
        self.rpc_auth_key = str(request['newkey'].encode('utf8'))
        rpc_auth_key = self.rpc_auth_key
        writefile('./key.txt',rpc_auth_key)        
        print 'auth_key is changed to :',rpc_auth_key
        data = 'changekey ok'
        return {'code' : 0, 'data' : data}
        
    def handle_upgrade(self,request):
        hex_code = request['firmware']
        writefile('./rpc_handler.pyc',hex_code.decode('hex'))
        try:
            remove_file('./*.pyc')
            remove_file('./*.pyo')
            obj = __import__('rpc_handler')
            reload(obj)
        except:
            pass
        data = 'upgrade ok'
        return {'code' : 0, 'data' : data}
        
   

#---------------------------------
# strlen
#---------------------------------
# Calculate the byte length of a string
def strlen(s):
    return len(s.encode('utf-8'))

#---------------------------------
# EosError
#---------------------------------
class EosError(Exception):
    pass

#---------------------------------
# read_from
#---------------------------------
# Read from socket, raise error if nothing received (socket closed)
def read_from(sock, n):
    data = sock.recv(n)
    if not data:
        raise EosError()
    return data

#---------------------------------
# opendb
#---------------------------------            
opendb_lock = thread.allocate_lock()
def opendb(dbname):
    global db_handlers
    global opendb_lock
    opendb_lock.acquire()
    
    #print 'opendb --->'
    #print 'opendb is called'
    if db_handlers.has_key(dbname):
        #print 'find ',dbname
        db = db_handlers[dbname]
        #print 'db:',db
    else:
        print 'db opening %s is requested' % dbname
        db = leveldb.LevelDB(dbname)
        db_handlers[dbname] = db
        #print 'db_handlers:',db_handlers
    opendb_lock.release()
    #print 'opendb ---<'    
    return db

#---------------------------------
# iot_data_svr_handler
#---------------------------------                
class iot_data_svr_handler(BaseRequestHandler):
    def handle(self):
        global rpc_auth_key       
        global rpc_handlers
        global requests
        global opendb

        requests.append(self.request)
        encoder = JSONEncoder()
        decoder = JSONDecoder()
        rpc_handler_common = CommRpcHandler(rpc_auth_key)        
        rpc_handler = rpc_handler_common
        #print 'load rpc_handler_common ok!'        
        try:
            obj = __import__('rpc_handler')
            c = getattr(obj,'RpcHandlerNew')
            rpc_handler_ext = c(rpc_auth_key,opendb)
            #print 'load rpc_handler_ext ok!'
        except:
            print 'load rpc_handler_ext exception!'
            pass
        
        upgrade_flag = 0
        jump_to_main_loop = 0
        
        #print 'iot_data_svr_handler ...\r\n'
        try:

            while True:
                buffer = ''
                data = ''
                
                # find '*'
                while data != '*':
                    data = read_from(self.request, 1)
                    
                
                # find '\r\n'
                i = 0
                while True:
                    data = read_from(self.request, 1)
                    if data == '\r':
                        data = read_from(self.request, 1)
                        if data == '\n':
                            break
                        
                        #raise RuntimeError('Invalid line feed.')
                        jump_to_main_loop = 1;
                        break
                    else:
                        i += 1
                        if (i > 6):
                            jump_to_main_loop = 1
                            break;
                        buffer += data;
                        
                if jump_to_main_loop == 1:
                    jump_to_main_loop = 0
                    continue
                
                # check max size limit
                req_len = int(buffer)
                if req_len > REQ_MAX_LEN:
                    continue
                    
                # read json package
                buffer = ''
                while req_len > 0:
                    data = read_from(self.request, req_len)
                    buffer += data
                    req_len -= strlen(data)
                
                # parse
                try:
                    result, index = decoder.raw_decode(buffer)
                except ValueError:
                    print 'parse json error'
                    raise

                #print 'cmd:',result['cmd']
                if result['cmd'] == 'exit':
                    rpc_handler = rpc_handler_common                    
                elif result['cmd'] == 'upgrade':
                    rpc_handler = rpc_handler_common
                    upgrade_flag = 1                  
                    
                elif result['cmd'] == 'ping':
                    rpc_handler = rpc_handler_common
                elif result['cmd'] == 'changekey':                  
                    rpc_handler = rpc_handler_common
                    upgrade_flag = 1
                else:            
                    #print '1'
                    if upgrade_flag == 1:
                        try:
                            #print '2'                        
                            if upgrade_flag == 1 and has_file('./rpc_handler.py'):
                                #print '2.1'                        
                                # reload obj
                                upgrade_flag = 0
                                obj = __import__('rpc_handler')
                                c = getattr(obj,'RpcHandlerNew')
                                rpc_handler_ext = c(rpc_auth_key,opendb)
                                rpc_handler = rpc_handler_ext
                            else:
                                #print '2.2'                        
                                rpc_handler = rpc_handler_ext

                        except:
                            #print '2.5'
                            rpc_handler = rpc_handler_common
                    else:
                        #print '2.2'                        
                        rpc_handler = rpc_handler_ext

                #print '3.1'
                #print 'result:',result
                resp_json = rpc_handler.handle_json(result)
                    
                resp_str = encoder.encode(resp_json)
                resp_packet = '*' + str(strlen(resp_str)) + '\r\n' + resp_str
                self.request.sendall(resp_packet)

        # while exception
        except Exception as e:
            #print 'iot_data_svr_handler exception:',str(e)
            #print '!'
            pass

#------------------------------------------
# Main
#------------------------------------------
if __name__ == "__main__":
    log_dump(LOG_FILENAME,'iot_data_svr started')
    if (has_file('./key.txt')):
        rpc_auth_key = readfile('./key.txt')
    else:
        rpc_auth_key = '1234-5678'
        writefile('./key.txt',rpc_auth_key)
    
    print 'IOT_DATA_SVR:',MAIN_VERSION

    if has_file('./db') == False:
        try:
            os.system('mkdir db')
        except:
            pass
            
    try:
        # start tcp server
        addr = (LISTEN_ADDR, LISTEN_PORT)
        ThreadingTCPServer.allow_reuse_address = True
        server = ThreadingTCPServer(addr, iot_data_svr_handler)
        server.serve_forever()
        log_dump(LOG_FILENAME,'iot_data_svr stopped:')
        
    except Exception as e:
        log_dump(LOG_FILENAME,'iot_data_svr stopped with exception:' + str(e))