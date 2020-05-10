#!/bin/bash
#by lantian 20190326
#用法： bash rds_check.sh db_name (多个db用空格隔开)


#setting connect information
src_host='127.0.0.1'
src_user='root'
src_passwd='rootroot'
#src_port=

dest_host='127.0.0.1'
dest_user='root'
dest_passwd='rootroot'
#dest_port=

#setting bassic var
databases=($*)
date=`date "+%F_%H_%M_%S"`


echo -n "正在校验"
for se in `seq 3`
do
	sleep 1
	if (( $se == 3 ))
	then
		echo  "."
	else
		echo -n "."
	fi
done
sleep 1
echo "开始时间：`date "+%F %H:%M:%S"`"

#遍历数组生成统计结果
for db in ${databases[@]}   
do
		#src count_sum
		for i in $(/usr/local/mysql/bin/mysql -h $src_host -u$src_user -p$src_passwd -e "select table_name from information_schema.tables where table_schema='$db';"|egrep -v "table_name");do /usr/local/mysql/bin/mysql -h $src_host -u$src_user -p$src_passwd -e "use $db;select count(*),'$i' from $i"|egrep -v "count";done >$db\_tables_countsum_$date\.src 2>/dev/null
		#src check_sum
		for i in $(/usr/local/mysql/bin/mysql -h $src_host -u$src_user -p$src_passwd -e "select table_name from information_schema.tables where table_schema='$db';"|egrep -v "table_name");do /usr/local/mysql/bin/mysql -h $src_host -u$src_user -p$src_passwd -e "use $db;checksum table $i"|sed -n '2p';done>$db\_tables_checksum_$date\.src 2>/dev/null
		#dest count_sum
		for i in $(/usr/local/mysql/bin/mysql -h $dest_host -u$dest_user -p$dest_passwd -e "select table_name from information_schema.tables where table_schema='$db';"|egrep -v "table_name");do /usr/local/mysql/bin/mysql -h $dest_host -u$dest_user -p$dest_passwd  -e "use $db;select count(*),'$i' from $i"|egrep -v "count";done >$db\_tables_countsum_$date\.dest 2>/dev/null
		#dest check_sum
		for i in $(/usr/local/mysql/bin/mysql -h $dest_host -u$dest_user -p$dest_passwd -e "select table_name from information_schema.tables where table_schema='$db';"|egrep -v "table_name");do /usr/local/mysql/bin/mysql -h $dest_host -u$dest_user -p$dest_passwd -e "use $db;checksum table $i"|sed -n '2p';done>$db\_tables_checksum_$date\.dest 2>/dev/null
done

#begin check
for db2 in ${databases[@]}
do
	diff $db2\_tables_countsum_$date\.src $db2\_tables_countsum_$date\.dest>$db2\_count.txt
	if [[ ! -s $db2\_count.txt ]]
	then
		echo -e "\tSchema $db2 count_sum is OK!"
		rm -f $db2\_count.txt
	else
		echo -e "\tSchema $db2 count_sum is NOT ok in file count.txt!"
	fi

	diff $db2\_tables_checksum_$date\.src $db2\_tables_checksum_$date\.dest>$db2\_checksum.txt
	if [[ ! -s $db2\_checksum.txt ]]
	then
    		echo -e "\tSchema $db2 check_sum is OK!"
		rm -f $db2\_checksum.txt
	else
    		echo -e "\tSchema $db2 count_sum is NOT OK in file checksum.txt!" 
	fi
	rm -f $db2\_tables_countsum_$date\.src $db2\_tables_countsum_$date\.dest $db2\_tables_checksum_$date\.src $db2\_tables_checksum_$date\.dest
done
echo "结束时间：`date "+%F %H:%M:%S"`"
echo "校验完成."

