for var in 1 2 3 4
do
        name="worker"
        vm=$name$var
        echo $vm
        ssh $vm 'rm -rf /home/hadoop/hadoop/logs/*'

done

ssh master 'rm -rf /home/hadoop/hadoop/logs/*'
