# -*- coding:utf-8 -*-


from odps import ODPS
import os,sys


'''
	1、默认源端要校验的表和分区在目标端均存在
	2、源端的表在目标端不存在则不校验该表，源端的分区在目标端不存在则不校验该分区
	3、用法：python 脚本.py 源项目名称 目标项目名称
'''

s = ODPS('LTAIz5oYFoQP3BJi', 'AURiYEhglmG8IINAklgOFZodL0HWGk', '%s'% sys.argv[1],endpoint='http://service.cn.maxcompute.aliyun.com/api')
d = ODPS('LTAIzUw6gMLapKNf', 'Y14A3T921OrjL0gVaWhuIAwToJ3nWr', '%s'% sys.argv[2],endpoint='http://service.cn.maxcompute.aliyun.com/api')

print ("######################################################################")

for table in  s.list_tables():
	t1=s.get_table(table.name)
	if d.exist_table(table.name):
		t2=d.get_table(table.name)
	else:
		print ("表%s 在目标项目%s中不存在 跳过校验" %(table.name,sys.argv[2]))
		continue

	if table.schema.partitions:  #判断该表是否为分区表
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
						print ("分区表%s数据不一致,分区%s 详情见%s_diff.txt"%(table.name,partition.name,table.name))
						with open (table.name+"_diff.txt",'a+') as f:
							f.write("表名：%s\t分区：%s\t源端数据量：%s\t目标数据量:%s\n"%(table.name,partition.name,count1,count2))
						#print "表名：%s\t分区：%s\t源端数据量：%s\t目标数据量:%s" %(table.name,partition.name,count1,count2)
					else:
						pass
			else:
				print('分区%s在目标表%s不存在' %(partition.name,table.name))
	else:
		print ('Table %s is not  partitioned.') % table.name
		with t1.open_reader() as reader:
			count1 = reader.count
		with  t2.open_reader() as reader2:
			count2=reader2.count
			if count1 != count2:
				print ("表名：%s\t源端数据量：%s\t目标数据量:%s") %(table.name,count1,count2)
			else:
				pass
		pass

		
