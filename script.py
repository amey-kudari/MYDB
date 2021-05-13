import numpy as np
import pandas as pd
import time
import json
import threading 

"""
DATABASE
"""
class database():
    def __init__(self, name):
        self.name = name+'.mydb'
        open(self.name, 'w')
    def insert(self, rrow):
        row = '|,|'.join([str(i) for i in rrow])
        open(self.name, 'a').write(row + '\n')
    def ret(self):
        return json.dumps([i.split('|,|') for i in open(self.name,'r').read().strip().split('\n')])
    def clear(self):
        open(self.name, 'w')
"""
END DATABASE
"""

fname = 'B.R.Project.xls'

def readfile(fname):
    data = pd.ExcelFile(fname)
    ndata = data.parse(data.sheet_names[0])
    rdata = np.array(ndata)
    rdata = list(rdata)
    ret = [
            list(a) 
            for a in rdata 
            if all([
                str(i)!='nan' 
                for i in a])
    ]
    for i in range(len(ret)):
        ret[i][1] = str(ret[i][1].to_pydatetime())
    return ret

ndb = database('mydb')
# ndb.createdb('mydb')
def insertfunction(row):
    ctime = time.time()
    """
    push into database
    """
    ndb.insert(row)
    """
    push completed
    """
    print(row, time.time()-ctime)

def simulator(fname, insf, tdiff):
    data = readfile(fname)
    for i in data:
        #time.sleep(tdiff)
        insf(i)

def simulator1(fname, insf, tdiff):
    data = readfile(fname)
    ctime = time.time()
    for i in data:
        #time.sleep(tdiff)
        ndb.insert(i)
    print('BULK INSERT (1 by 1) OF ', len(data), 'ROWS:', time.time()-ctime, 'SECONDS')

def tinsertf(data):
    for i in data:
        ndb.insert(i)

def tsimulator(fname, threads):
    data = readfile(fname)
    n = len(data)
    st = n//threads
    trdsd = []
    for i in range(threads):
        trdsd.append(data[i*st:i*st+st])
    trds = []
    for i in range(threads):
        trds.append(threading.Thread(target=tinsertf, args = (trdsd[i], )))
        trds[i].start()
    ctime = time.time()
    for i in range(threads):
        trds[i].join()
    print('MULTI THREADED BULK INSERT OF ', len(data), 'ROWS:', time.time()-ctime, 'SECONDS')

ndb.clear()
simulator(fname, insertfunction, 0.1)
ndb.clear()
simulator1(fname, insertfunction, 0.1)
ndb.clear()
tsimulator(fname, 8)
