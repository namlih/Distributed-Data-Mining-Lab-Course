source='/home/hadoop/spark/conf/*'
destination='/home/hadoop/spark/conf/'
for var in 1 2 3 4
do
	scp $source hadoop@worker$var:$destination
done

