for var in 1 2 3 4
do
	name="worker"
	vm=$name$var
	echo $vm
	ssh $vm 'rm -rf /tmp/hadoop-hadoop/*'
	ssh $vm 'rm -r /home/hadoop/data/dataNode/current/'
	ssh $vm 'rm -r /mnt2/current/'
	ssh $vm 'rm -r /mnt3/current/'
	ssh $vm 'hdfs datanode -format'
done

# THis is for master 
rm -rf /tmp/hadoop-hadoop/*
rm -r /home/hadoop/data/dataNode/current/
rm -r /mnt4/current/
rm -rf /home/hadoop/data/nameNode

echo "Y"|hdfs namenode -format
hdfs datanode -format
