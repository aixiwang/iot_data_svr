#-----------------------------------------------------------
# Copyright (c) 2015 by Aixi Wang <aixi.wang@hotmail.com>
#-----------------------------------------------------------
try:
    from pathconfig import *
except:
    pass

from pyiotlib import *
import time
import sys


#----------------------
# main
#----------------------
if __name__ == "__main__":
    import thread

    rpc_handler = app_sdk('1234-1',server_ip='192.168.2.104',server_port=7777)   
    t1 = time.time()
    
    ok_cnt = 0
    fail_cnt = 0
    
    for i in xrange(1000000):
        json_out = rpc_handler.set(str(i),str(i))
        if json_out['code'] < 0:
            print '!!!test set fail'
            print json_out
            fail_cnt += 1
        else:
            json_out = rpc_handler.get(str(i))
            if json_out['code'] < 0:
                print '!!!test get fail'
                print json_out
                fail_cnt += 1
            else:
                print json_out
                if json_out['data']['v'] == str(i):
                    if (i+1)%1000 == 0:
                        print i,' passed'
                    ok_cnt += 1
                else:
                    if (i+1)%1000 == 0:
                        print i,' ------------------'
                    fail_cnt += 1

    print 'ok_cnt:',ok_cnt
    print 'fail_cnt:',fail_cnt
    s=raw_input('any key')
