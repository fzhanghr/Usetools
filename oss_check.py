# -*- coding:utf-8 -*-
# By lantian
'''
1、基于osstuil作校验，请先安装此工具，如下linux上安装配置ossutil
        1)下载工具包
                #wget http://gosspublic.alicdn.com/ossutil/1.4.2/ossutil64
        2)修改为可执行权限
                #chmod 755 ossutil64
        3)生成配置文件(两个文件：分别用于源端和目的端访问),并配置连接信息：
                #./{安装包存在路径}/ossutil64 config --config-file={配置文件存放路径}.ossutilconfig
其他安装见：https://help.aliyun.com/document_detail/50452.html?spm=a2c4g.11186623.6.1294.715869013jVV1c
2、用法:python oss_check.py 源bucket名字 目的bucket名字
'''

import sys,os
import time

class OSS_CHECK():
        def __init__(self):
                self.src_config=".safp_3.3_ossutilconfig"   #源oss配置文件
                self.dest_config=".safp_3.8_ossutilconfig" #目的oss配置文件
        def check(self):
                d_data = os.popen('''./ossutil64 ls oss://%s --config-file=%s|egrep  "^[0-9]{4,5}"|awk -F " " '{if($8!~/\/$/) {print "File:"$8,$7,$5}}'|cut -d "/" -f 4- '''%(sys.argv[2],self.dest_config)).readlines()
                s_data = os.popen('''./ossutil64 ls oss://%s --config-file=%s|egrep  "^[0-9]{4,5}"|awk -F " " '{if($8!~/\/$/) {print "File:"$8,$7,$5}}'|cut -d "/" -f 4- '''%(sys.argv[1],self.src_config)).readlines()
                if len(s_data)==0:
                        print "Bucket:%s不存在或内容为空"%sys.argv[1]
                        sys.exit()
                elif len(d_data)==0:
                        print "Bucket:%s不存在或内容为空"%sys.argv[2]
                        sys.exit()

                insnotd,indnots=[x.strip() for x in s_data if not x  in d_data],[x.strip() for x in d_data if not x  in s_data]
                if len(insnotd)==0 and len(indnots)==0:
                        print'数据一致！'
                else:
                        print'数据不一致:'
                        if len(insnotd)==0 and len(indnots)>0:
                                if len(indnots)<=50:
                                        print 'Bucket:%s 缺少%s个文件' %(sys.argv[1],len(indnots))
                                        for ob in indnots:
                                                print ob.split()[0]
                                else:
                                        print 'Bucket:%s 缺少%s个文件' %(sys.argv[1],len(indnots))
                                        with open("/tmp/indnots.txt",'w') as f:
                                                f.write(indnots)
                        elif len(indnots)==0 and len(insnotd)>0:
                                print "Bucket:%s 缺少%s个文件" %(sys.argv[2],len(insnotd))
                                for ob in insnotd:
                                        print ob.split()[0]
                        else:
                                if len(insnotd)==len(indnots):
                                        print "Bukcet:%s \n" %sys.argv[1],insnotd
                                        print "Bukcet:%s \n" %sys.argv[2],indnots
                                else:
                                        print "Bucket:%s 缺少%s个objects:" %(sys.argv[1],len(indnots))
                                        for ob in indnots:
                                                print ob.split()[0]
                                        print "\n"
                                        print "Bucket:%s 缺少%s个objects:" %(sys.argv[2],len(insnotd))
                                        for ob in insnotd:
                                                print ob.split()[0]

        def usage(self):
                print "Usage:"
                print "\t python 源bucket 目的bucket"

if __name__=='__main__':
        print time.asctime(time.localtime(time.time()))
        if not len(sys.argv)==3:
                oss=OSS_CHECK()
                oss.usage()
                print time.asctime(time.localtime(time.time()))
        else:
                oss=OSS_CHECK()
                oss.check()
                print time.asctime(time.localtime(time.time()))
