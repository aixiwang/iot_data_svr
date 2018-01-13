# -*- coding=gbk -*- 
#-----------------------------------------------------------
# Copyright (c) 2015 by Aixi Wang <aixi.wang@hotmail.com>
#-----------------------------------------------------------
import random, time
import os
import sys

import thread, threading, subprocess
#import mosquitto
import leveldb

RPC_VER = '0.4'
print 'RPC_VER:',RPC_VER

#import memcacheq
#q_rt_data = memcacheq.connect('q_rt_data')

#------------------------------
# set_kv
#------------------------------
def set_kv(db,k,v):
    db.Put(k,v)
    
#------------------------------
# get_kv
#------------------------------
def get_kv(db,k):
    #print 'k:',k
    try:
        v = db.Get(k)
        return v
    except:
        return ''
#------------------------------
# rm_kv
#------------------------------
def rm_kv(db,k):
    db.Delete(k)
    return
    
#------------------------------
# get_search_keys_old
#------------------------------
def get_search_keys_old(db,tag,name,t1,t2):   
    if name != '*':
        prefix = tag + ':' + name      
    else:
        prefix = tag + ':'

    datas = []
    f_t1 = float(t1)
    f_t2 = float(t2)
    
    try:
        if name != '*':
            # to be optimized
            kv = list(db.RangeIter(key_from = None, key_to = None))
            
        else:
            kv = list(db.RangeIter(key_from = None, key_to = None))
            
        for record in kv:
            try:
                #print record
                if record[1][0:len(prefix)] == prefix:
                    f_s = record[1].split(':')
                    #print 'f_s:',f_s           
                    t = float(f_s[2])                            
                    if t > (f_t2):
                        break
                        
                    if t >= f_t1 and t <= f_t2:
                        #print 'append key:',record[1]
                        datas.append(record[1])
            except:
                pass
        return {'code' : 0,'data':datas}
    except Exception as e:
        print '@'
        return {'code' : -1,'data':'exception:' + str(e)}




#------------------------------
# get_search_keys
#------------------------------
def get_search_keys(db,tag,name,t1,t2):   
    if name != '*':
        prefix = tag + ':' + name      
    else:
        prefix = tag + ':'

    datas = []
    f_t1 = float(t1)
    f_t2 = float(t2)
    
    
    k_t1 = int(f_t1)/60
    k_t2 = int(f_t2)/60
    
    try:
        for k in range(k_t1,k_t2 + 1):
            keys_value = get_kv(db,str(k))
            #print 'keys_value:',keys_value
            if keys_value != '':
                keys_array = keys_value.split('#')
                for key_str in keys_array:
                    try:
                        #print 'key_str:',key_str
                        if key_str[0:len(prefix)] == prefix:
                            f_s = key_str.split(':')
                            #print 'f_s:',f_s           
                            t = float(f_s[2])                            
                            if t > (f_t2):
                                break
                                
                            if t >= f_t1 and t <= f_t2:
                                #print 'append key:',record[1]
                                datas.append(key_str)
                    except Exception as e:
                        print 'search keys exception:', str(e)
                        pass

        #print 'search_keys:',datas
        return {'code' : 0,'data':datas}

    except Exception as e:
        print 'get_search_keys exception: ' + str(e)
        return {'code' : -1,'data':'exception:' + str(e)}
        
#------------------------------
# delete_search_key
#------------------------------
def delete_search_key(db,key):
    rm_kv(db,key)

#------------------------------
# opendb
#------------------------------
def opendb(dbname):
    db = leveldb.LevelDB(dbname)
    return db    

class RpcHandlerNew:
    def __init__(self,auth_key,opendb):
        self.rpc_auth_key = auth_key
        self.key = ''
        self.count = 0
        self.opendb = opendb
        self.userdb = None
        pass
    def shutdown(self):
        try:
            print 'rpc_handler shutdown'
            self.db.Close()
            self.index_db.Close()
            self.stats_db.Close()
            self.index_stats_db.Close()
        except:
            print 'rpc_handler shutdown exception'
            pass

        
    #------------------------------
    # handle_json
    #------------------------------        
    def handle_json(self, request):
        #print 'request:',request

        try:
        #if 1:
            key = request["key"]
            if self.userdb == None:
                self.user_db = self.opendb('./db/user.db')
            self.sensor_key = self.user_db.Get(key)
            #print 'sensor_key:',sensor_key
            if key != self.rpc_auth_key and self.sensor_key != 'sensor':
                print 'key:',key
                time.sleep(5)
                return {'code' : -1, 'data' : 'invalid key'}
            if key != self.rpc_auth_key:
                if key.find(':') > 0:
                    key = key.split(':')[0]
            
            if self.key != key:   
                self.key = key
                self.dbname = './db/' + key + '.db'
                self.index_dbname = './db/' + key + '.idx.db'
                self.stats_dbname = './db/' + key + '-stats' + '.db'
                self.index_stats_dbname = './db/' + key + '-stats' + '.idx.db'
                
                #print 'init level db start, key=',self.key

                self.db = self.opendb(self.dbname)
                self.index_db = self.opendb(self.index_dbname)
                self.stats_db = self.opendb(self.stats_dbname)
                self.index_stats_db = self.opendb(self.index_stats_dbname)
                #sensor_key = get_kv('./db/user.db',key)
                

                #print 'init level db end'
                
            cmd = request["cmd"]
            if (cmd.find('bsddb_') == 0):
                cmd = cmd[6:]
                
            if cmd == 'get_rpc_ver':            
                return self.handle_get_rpc_ver(request)
            if cmd == 'echo':            
                return self.handle_echo(request)
            if cmd == 'setfile':
                return self.handle_setfile(request)
            if cmd == 'getfile':            
                return self.handle_getfile(request) 
            elif cmd == 'set':
                return self.handle_set(request)
            elif cmd == 'get':
                return self.handle_get(request)
            elif cmd == 'delete':
                return self.handle_delete(request)
            elif cmd == 'set_ts_data':
                return self.handle_set_ts_data(request)
            elif cmd == 'get_ts_datas':
                return self.handle_get_ts_datas(request)
            elif cmd == 'get_ts_keys':
                return self.handle_get_ts_keys(request)
            elif cmd == 'set_stats_data':
                return self.handle_set_stats_data(request)
            elif cmd == 'delete_stats':
                return self.handle_delete_stats(request)
            elif cmd == 'get_stats_datas':
                return self.handle_get_stats_datas(request)
            elif cmd == 'get_stats_keys':
                return self.handle_get_stats_keys(request)
            elif cmd == 'mqtt_pub':
                return self.handle_mqtt_pub(request)
                
            else:
                return {'code' : -1, 'data' : 'unkonwn command'}
                
        except Exception as e:
            print 'rpc_handler exception:',str(e)
            return {'code' : -1,'data':str(e)}

    #------------------------------
    # handle_get_rpc_ver
    #------------------------------
    def handle_get_rpc_ver(self, request):
        return {'code' : 0, 'data' : RPC_VER}
        
    #------------------------------
    # handle_echo
    #------------------------------
    def handle_echo(self, request):
        data = request['in'] + '-echo'
        return {'code' : 0, 'data' : data}
        
    #------------------------------
    # handle_setfile
    #------------------------------       
    def handle_setfile(self, request):
        d = './files/' + self.key + '/'
        if os.path.exists(d) == False:
            os.system('mkdir ' + d)        
        fname = d + request['filename']
        f_content = request['content'].decode('hex')
        f = open(fname,'wb')
        f.write(f_content)
        f.close()       
        return {'code' : 0, 'data':None}

    #------------------------------
    # handle_getfile
    #------------------------------       
    def handle_getfile(self, request):
        d = './files/' + self.key + '/'
        if os.path.exists(d) == False:
            os.system('mkdir ' + d)
        fname = d + request['filename']
        f = open(fname,'rb')
        f_content = f.read().encode('hex')
        f.close()       
        return {'code' : 0,'data': f_content}
        
    #------------------------------
    # handle_set
    #------------------------------       
    def handle_set(self, request):
        k = request['k'].encode('utf8')
        v = request['v'].encode('utf8')
        set_kv(self.db,k,v)
        return {'code' : 0,'data':None}
 
    #------------------------------
    # handle_get
    #------------------------------       
    def handle_get(self, request):
        k = request['k'].encode('utf8')    
        v = get_kv(self.db,k)
        return {'code' : 0,'data':{'k':k,'v':v}}
        
    #------------------------------
    # handle_delete
    #------------------------------       
    def handle_delete(self, request):
        k = request['k'].encode('utf8')    
        rm_kv(self.db,k)
        return {'code' : 0,'data':None}

    #------------------------------
    # handle_set_ts_data
    #------------------------------       
    def handle_set_ts_data(self, request):
        tag = request['tag']
        name = request['name']
        v = request['v']
        if request.has_key('t'):
            t_str = request['t']
        else:
            #self.count = self.count + 1
            #if self.count >
            t_str = str(time.time())
        #idx = t_str.find('.')
        #if idx > 0 and (len(s_str)-idx) == 3:
        #    t_str += '0'
        
        k = str(tag) + ':' + str(name) + ':' + t_str
        s = name + '\n' + t_str + '\n' + v
        #if (tag == 'data'):
        #    q_rt_data.add(s)
        set_kv(self.db,k,v)       
        index_key = str(int(float(t_str))/60)
        index_value = get_kv(self.index_db,index_key)
        if index_value == '':
            index_value = k
        else:
            index_value += '#' + k
        
        #print 'save index_db:',index_key,index_value
        set_kv(self.index_db,index_key,index_value)
        set_kv(self.db,k,v)
        
        return {'code' : 0,'data':None}

    #------------------------------
    # handle_get_ts_datas
    #------------------------------       
    def handle_get_ts_datas(self, request):
        t1 = float(request['t1'])
        t2 = float(request['t2'])
        tag = request['tag']
        name = request['name']
        
        keys = get_search_keys(self.index_db,tag,name,t1,t2)
        #print 'keys:',keys
        if keys['code'] != 0:
            return {'code':-1,'data':'search key error!'}
            
        datas =[]
        for k in keys['data']:
            #print 'k:',i,' v:',self.db[i]
            v = get_kv(self.db,k)
            rec = [k,v]
            datas.append(rec)
        return {'code':0,'data':datas}

    #------------------------------
    # handle_get_ts_keys
    #------------------------------       
    def handle_get_ts_keys(self, request):    
        t1 = float(request['t1'])
        t2 = float(request['t2'])
        tag = request['tag']
        name = request['name']
        keys = get_search_keys(self.index_db,tag,name,t1,t2)
        return keys
    
    #------------------------------
    # handle_set_stats_data
    #------------------------------       
    def handle_set_stats_data(self, request):
        tag = request['tag']
        name = request['name']
        time = request['time']
        v = request['v']
        k = '%s:%s:%s' % (str(tag),str(name),str(time))
        
        set_kv(self.stats_db,k,v)
        set_kv(self.index_stats_db,str(time),k)
        
       
        return {'code' : 0,'data':None}
    
    #------------------------------
    # handle_delete_stats
    #------------------------------       
    def handle_delete_stats(self, request):
        k = str(request['k'])
        rm_kv(self.stats_db,k)
        return {'code' : 0,'data':None}
    
    #------------------------------
    # handle_get_stats_datas
    #------------------------------       
    def handle_get_stats_datas(self, request):
        t1 = float(request['t1'])
        t2 = float(request['t2'])
        tag = request['tag']
        name = request['name']

        keys = get_search_keys(self.index_stats_db,tag,name,t1,t2)
        if keys['code'] != 0:
            return {'code':-1,'data':'search key error!'}
            
        datas =[]

        for k in keys['data']:
            #print 'k:',i,' v:',self.db[i]
            v = get_kv(self.stats_db,k)
            rec = [k,v]
            datas.append(rec)
            
        return {'code':0,'data':datas}    
        
    #------------------------------
    # handle_get_stats_keys
    #------------------------------       
    def handle_get_stats_keys(self, request):    
        t1 = float(request['t1'])
        t2 = float(request['t2'])
        tag = request['tag']
        name = request['name']
        keys = get_search_keys(self.index_stats_db,tag,name,t1,t2)
        return keys
        
    #-------------------
    # handle_mqtt_pub
    #-------------------
    def handle_mqtt_pub(self,request):
        server_addr = request['server_addr']
        server_port = request['server_port']
        username = request['username']
        password = request['password']
        topic = request['topic']
        message = request['message']    
        mqttc = mosquitto.Mosquitto("gateway-" + str(time.time()))
        mqttc.username_pw_set(username,password)
        mqttc.connect(server_addr, server_port, 60)
        result, mid = mqttc.publish(str(topic), str(message),0)
        if (result == mosquitto.MOSQ_ERR_SUCCESS):
            retcode = 0
        else:
            retcode = -1
        res = {
                "code" : retcode,
                "data" : None
              }                 
        return res
#----------------------
# main
#----------------------
if __name__ == "__main__":
    rpc = RpcHandlerNew('1234-5678',opendb)

    # echo
    json_in = {
             'key':'1234-5678',
             'cmd':'echo',
             'in':'asdfasdfasdf',
              }
    json_out = rpc.handle_json(json_in)
    print json_out
    
    # setfile
    print '1.test setfile'
    json_in = {
             'key':'1234-5678',
             'cmd':'setfile',
             'filename': './test.txt',
             'content': '343434343434343434343434343434',
               }
    json_out = rpc.handle_json(json_in)
    print json_out
    
    # getfile
    print '2.test getfile'    
    json_in = {
             'key':'1234-5678',
             'cmd':'getfile',
             'filename': './test.txt',
              }
    json_out = rpc.handle_json(json_in)
    print json_out

    # test set
    print '3.test set'
    json_in = {
             'key':'1234-5678',    
             'cmd':'set',
             'k':'asdfasdfasdfasd',
             'v':'adsfasdfasd==============asdfa',
              }
    json_out = rpc.handle_json(json_in)
    print json_out    

    #-------------------------------------------    
    # test get
    print '4.test get'
    json_in = {
             'key':'1234-5678',    
             'cmd':'get',
             'k':'asdfasdfasdfasd',
              }
    json_out = rpc.handle_json(json_in)
    print json_out

    #-------------------------------------------    
    # test delete
    print '5.test delete'
    json_in = {
             'key':'1234-5678',    
             'cmd':'delete',
             'k':'asdfasdfasdfasd',
              }
    json_out = rpc.handle_json(json_in)
    print json_out
    
    #-------------------------------------------    
    # test get
    print '6.test get'
    json_in = {
             'key':'1234-5678',    
             'cmd':'get',
             'k':'asdfasdfasdfasd',
              }
              
    try:
        json_out = rpc.handle_json(json_in)
        print json_out
        print 'error!, the key has not been deleted'
        
    except:
        print 'yes, the key have been deleted'
        
    # set_ts_data
    print '7.set_ts_data, t1'
    json_in = {
             'key':'1234-5678',
             'cmd':'set_ts_data',
             'tag': 'data',
             'name': 't1',
             'v':'32.234',
               }
    json_out = rpc.handle_json(json_in)
    print json_out
    
    # set_ts_data
    print '7.1 test set_ts_data, t1'
    json_in = {
             'key':'1234-5678',
             'cmd':'set_ts_data',
             'tag': 'data',
             'name': 't1',
             'v':'12.2',
               }
    json_out = rpc.handle_json(json_in)
    print json_out
    
    # set_ts_data
    print '7.2 test set_ts_data, t2'
    json_in = {
             'key':'1234-5678',
             'cmd':'set_ts_data',
             'tag': 'data',
             'name': 't2',
             'v':'15',
               }
    json_out = rpc.handle_json(json_in)
    print json_out
    
    # get_ts_keys
    print '8. test get_ts_keys, *'
    json_in = {
             'key':'1234-5678',
             'cmd':'get_ts_keys',
             'tag': 'data',
             'name': '*',
             't1': time.time()-3600,
             't2': time.time(),
               }
    json_out = rpc.handle_json(json_in)
    print json_out
    
    # get_ts_datas
    print '9.1 test get_ts_datas, *'
    json_in = {
             'key':'1234-5678',
             'cmd':'get_ts_datas',
             'tag': 'data',
             'name': '*',
             't1': time.time()-3600,
             't2': time.time(),
               }
    json_out = rpc.handle_json(json_in)
    print json_out
   
    # get_ts_datas
    print '9.2 test get_ts_datas, t1'
    json_in = {
             'key':'1234-5678',
             'cmd':'get_ts_datas',
             'tag': 'data',
             'name': 't1',
             't1': time.time()-3600,
             't2': time.time(),
               }
    json_out = rpc.handle_json(json_in)
    print json_out
    
    #-------------------------------------------    
    # test mqtt pub
    print '10.test mqtt pub'           
    # test mqtt publish
    json_in = {
           'key':'1234-5678',      
           'cmd':'mqtt_pub',        
           'server_addr':'127.0.0.1',
           'server_port': 1883,
           'username':'',
           'password':'',
           'topic':'testmqttpub',                   
           'message':'asdfasdfasdfasdf',
          }
    json_out = rpc.handle_json(json_in) 
    print json_out
     
 
