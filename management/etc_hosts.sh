for var in 1 2 3 4
do
	name="worker"
	vm=$name$var
	echo $vm
	#scp /etc/hosts $vm:/etc/hosts
	ssh $vm 'rm -r /home/hadoop/spark'
	scp /home/hadoop/spark $vm:/home/hadoop/
done

