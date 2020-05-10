# -*- coding:utf-8 -*-
'''
1、odps校验脚本
2、usage：python 脚本 源项目名 目标项目名
3、提前安装好pyodps：pip install pyodps

'''
from odps import ODPS
import os,sys


s = ODPS('ak', 'secrect', '%s'% sys.argv[1],endpoint='http://service.cn.maxcompute.aliyun.com/api')
d = ODPS('ak', 'secrect', 'waq_std',endpoint='http://service.cn.maxcompute.aliyun.com/api')

print "#######################################################################"

for table in  s.list_tables():
	t1=s.get_table(table.name)
	if d.exist_table(table.name):
		t2=d.get_table(table.name)
	else:
		#print "目标表%s不存在,跳过校验"%table.name
		continue
	#判断该表是否为分区表
	if table.schema.partitions:
 		#print 'Table %s is partitioned.' %table.name
		for partition in table.partitions:
			#print partition.name
			with t1.open_reader(partition='%s' %partition.name) as reader:
				count1 = reader.count
				#print "表名：%s\t分区：%s\t数据量：%s" %(table.name,partition.name,count1)
			if t2.exist_partition(partition.name):
				with  t2.open_reader(partition='%s' %partition.name) as reader2:
					count2=reader2.count
					if count1 != count2:
						print "分区表%s数据不一致,分区%s 详情见%s_diff.txt"%(table.name,partition.name,table.name)
						with open (table.name+"_diff.txt",'a+') as f:
							f.write("表名：%s\t分区：%s\t源端数据量：%s\t目标数据量:%s\n"%(table.name,partition.name,count1,count2))
						#print "表名：%s\t分区：%s\t源端数据量：%s\t目标数据量:%s" %(table.name,partition.name,count1,count2)
					else:
						pass
			else:
				pass
	else:
		#print 'Table %s is not  partitioned.' % table.name
		with t1.open_reader() as reader:
			count1 = reader.count
		with  t2.open_reader() as reader2:
			count2=reader2.count
			if count1 != count2:
				print "表名：%s\t源端数据量：%s\t目标数据量:%s" %(table.name,count1,count2)
			else:
				pass
			
		
