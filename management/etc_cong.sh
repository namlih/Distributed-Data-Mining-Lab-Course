source='/home/hadoop/hadoop/etc/hadoop/*'
destination='/home/hadoop/hadoop/etc/hadoop/'
for var in 1 2 3 4
do
	scp $source hadoop@worker$var:$destination
done

