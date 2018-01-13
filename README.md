# IOT DATA SERVICE (iot_data_svr)
* Leveldb based json rpc server as IoT data service

## Features
* key/value pair
* time series data
* alarm data
* general file 
* 



## Revision History
[v01 2016-12-25 Aixi Wang]
* initial checkin


## Architecture
* json_rpc interface <--> leveldb


## How to test
* prepare -- pip install leveldb
* create account -- python keytool.py
* run service -- python iot_data_svr.py
* test service interface -- python test_data_read_write.py

### Sample test code
```python
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
    import thread


#----------------------
# main
#----------------------
if __name__ == "__main__":
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
                #print json_out
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
```

## TODO List
* TBD


## Contact me
* aixi.wang@hotmail.com


## Donate
* Bitcoin address: 1Aixi7ZpzQGMzVjx39GFCxVWpLpjTpcPgr
