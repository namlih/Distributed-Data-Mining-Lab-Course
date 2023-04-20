source='/home/hadoop/hadoop/etc/hadoop/*'
destination='/home/hadoop/hadoop/etc/hadoop.bak2/'

var='mkdir /home/hadoop/hadoop/etc/hadoop.bak2'
var2='cp -r /home/hadoop/hadoop/etc/hadoop /home/hadoop/hadoop/etc/hadoop.bak2/'
for i in 1 2 3 5 6
do
	ssh hadoop@group2worker1.dyn.mwn.de $var2
done
ssh hadoop@worker7 $var2
ssh hadoop@worker8 $var2
