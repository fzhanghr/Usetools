# -*- coding:utf-8 -*-

from odps import ODPS
import os,sys


odps = ODPS('AK', 'Secret', 'lantian',endpoint='http://service.cn.maxcompute.aliyun.com/api')
Dodps = ODPS('AK', 'Secret', 'lantian',endpoint='http://service.cn.maxcompute.aliyun.com/api')

class checksum(object):
    #非分区表校验
    def isnotPartition(self):
        with open("%s" % sys.argv[1], 'r') as f:
            tables = ([x.split() for x in f.readlines()])
            Scount, Dcount, lins= {}, {},0
            for table in tables:
                St, Dt = odps.get_table(table[0]), Dodps.get_table(table[0])
                with St.open_reader() as reader:
                    Scount[table[0]] = reader.count
                with Dt.open_reader() as reader:
                    Dcount[table[0]] = reader.count
            for key, value in Scount.items():
                if key in Dcount.keys() and value in Dcount.values():
                    print("表%s数据一致,数据行数%s"%(key,value))
                    lins=lins+1
                    if len(Dcount)==lins:
                        print("校验完成！")
                else:
                    print('差异表信息如下：{源表名:count,目标表名:count}')
                    print("\t\t{'%s:%s','%s:%s'}" % (key, value, key, Dcount[key]))

    #分区表校验
    def isPartition(self):
        with open("%s" % sys.argv[1], 'r') as f:
            tables = ([x.split() for x in f.readlines()])
            Scount, Dcount,lins = {}, {},0
            for table in tables:
                St, Dt = odps.get_table(table[0]), Dodps.get_table(table[0])
                with St.open_reader(sys.argv[2]) as reader:
                    Scount[table[0]] = reader.count
                with Dt.open_reader(sys.argv[2]) as reader:
                    Dcount[table[0]] = reader.count
            for key, value in Scount.items():
                if key in Dcount.keys() and value in Dcount.values():
                    print("表%s数据一致,数据行数%s" % (key, value))
                    lins = lins + 1
                    if len(Dcount) == lins:
                        print("校验完成！")
                else:
                    print('差异表信息如下：{源表名:count,目标表名:count}')
                    print("\t\t{'%s:%s','%s:%s'}" % (key, value, key, Dcount[key]))

    def usage(self):
        print("Usage:")
        print("\tpython odps_check.py 表信息所在文件名 [partition='']")
        print('''前提:
     1、安装PyODPS
        PyODPS支持Python2.6以上（包括Python3），系统安装pip后，只需运行pip install pyodps，PyODPS的相关依赖便会自动安装。
     2、分别配置好源和目标的连接信息''')



if __name__ == '__main__':
    checksum = checksum()
    if len(sys.argv)==1:
        checksum.usage()
    elif len(sys.argv) == 2 :
        if os.path.isfile(sys.argv[1]):
            print("正在执行...")
            checksum.isnotPartition()
        else:
            print("文件 %s 不存在! 该文件包含校验的表名，以换行符分割。"%sys.argv[1])
    elif len(sys.argv) == 3:
        if os.path.isfile(sys.argv[1]):
            print("正在执行...")
            checksum.isPartition()
        else:
            print("文件 %s 不存在! 该文件包含校验的表名，以换行符分割。"%sys.argv[1])
    else:
        checksum.usage()

