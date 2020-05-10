# -*- coding:utf-8 -*-

import oss2,sys,os

'''
1、安装SDK 参考地址：https://help.aliyun.com/document_detail/85288.html?spm=a2c4g.11186623.6.702.7b356901mvQxoz
2、配置源和目标的比连接信息sbucket和dbucket
3、运行脚本：python oss_check.py 源bucket 目标bucket
'''

sbucket = oss2.Bucket(oss2.Auth('LTAIzUw6gMLapKNf', 'Y14A3T921OrjL0gVaWhuIAwTo'),
                      'oss-cn-shanghai.aliyuncs.com', '%s' % sys.argv[1])
dbucket = oss2.Bucket(oss2.Auth('LTAIzUw6gMLapKNf', 'Y14A3T921OrjL0gVaWhuIAwTo'),
                      'oss-cn-shanghai.aliyuncs.com', '%s' % sys.argv[2])
s, d, count = {}, {}, 0

def CalculateFolderLength(sbucket, folder):
    length = 0
    for obj in oss2.ObjectIterator(sbucket, prefix=folder, max_keys=1000):
        length += obj.size
    return length

for obj in oss2.ObjectIterator(sbucket, delimiter='/'):
    if obj.is_prefix():  # 文件夹
        length = CalculateFolderLength(sbucket, obj.key)
        # print('directory: ' + obj.key + '  length:' + str(length / 1024) + "KB")
        s["文件夹"+obj.key]=str(float(length) / 1024) + "KB"
    else: # 文件
        s[obj.key]=str(float(obj.size) / 1024) + "KB"
        # print('file:' + obj.key + '  length:' + str(obj.size / 1024) + "KB")

for obj in oss2.ObjectIterator(dbucket, delimiter='/'):
    if obj.is_prefix():  # 文件夹
        length = CalculateFolderLength(dbucket, obj.key)
        # print('directory: ' + obj.key + '  length:' + str(length / 1024) + "KB")
        d["文件夹"+obj.key]=str(float(length) / 1024) + "KB"
    else: # 文件
        d[obj.key]=str(float(obj.size) / 1024) + "KB"
        # print('file:' + obj.key + '  length:' + str(obj.size / 1024) + "KB")




for file,size in s.items():
    if len(s)==len(d):
        if file in d.keys() and size in d.values():
            count=count+1
            if len(d)==count:
                print("校验完成，数据一致！")
        elif file in d and size not in d:
            with open("/test/logs",'a+') as f:
                print(r"Bucket:%s：%s-->%s  Bucket:%s：%s-->%s" % (sys.argv[1],file, size,sys.argv[2],file, d[file]))
                f.write("Bucket:%s：%s-->%s  Bucket:%s：%s-->%s" % (sys.argv[1],file, size,sys.argv[2],file, d[file]))
        else:
            print("%s在bucket:%s中不存在"%(file,sys.argv[2]))





