# -*- coding: utf-8 -*- 
#-----------------------------------------------------------
# Copyright (c) 2015 by Aixi Wang <aixi.wang@hotmail.com>
#-----------------------------------------------------------
import random, time
import os
import sys
import uuid

import leveldb
db = leveldb.LevelDB('./db/user.db')
#------------------------------
# set_kv
#------------------------------
def set_kv(k,v):
    global db
    db.Put(k,v)
    
#------------------------------
# get_kv
#------------------------------
def get_kv(k):
    global db

    v = db.Get(k)
    return v

#------------------------------
# rm_kv
#------------------------------
def rm_kv(k):
    global db
    db.Delete(k)
    return

#------------------------------
# list_kv
#------------------------------
def list_kv():
    global db
    kv = list(db.RangeIter(key_from = None, key_to = None))
    print kv


#------------------------------
# help
#------------------------------    
def help():
    print 'supported commands:'
    print '1.createuser'
    print '2.getuser'
    print '3.listusers'
    print '4.help'

#------------------------------
# createuser
#------------------------------     
def createuser():
    k = raw_input('please input key:')
    v = raw_input('please input role(sensor,--):')
    set_kv(k,v)
    v = get_kv(k)
    print 'k:',k,' read role from db:',v 
    
    pass
    
#------------------------------
# getuser
#------------------------------      
def getuser():
    k = raw_input('please input key:')
    v = get_kv(k)
    print 'k:',k,' read role from db:',v     

    pass
    
#------------------------------
# setuser
#------------------------------       
def setuser():
    k = raw_input('please input key:')
    v = get_kv(k)
    print 'k:',k,' read role from db:',v     
    v = raw_input('please input new role(admin,sensor,--):')
    set_kv(k,v)
    print 'k:',k,' read role from db:',v
    
    pass

#------------------------------
# listusers
#------------------------------     
def listusers():
    list_kv()
    pass
    

#----------------------
# main
#----------------------
if __name__ == "__main__":
    print 'key tool v0.1, ctrl+c to exit.'
    while(1):
        print '---------------------------------------------'
        cmd = raw_input('please input your command:')
        if (cmd == 'createuser'):
            createuser()
        elif (cmd == 'getuser'):
            getuser()
        elif (cmd == 'setuser'):
            setuser()  
        elif (cmd == 'listusers'):
            listusers() 
        else:
            help()
