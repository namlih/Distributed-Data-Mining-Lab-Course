for var in 1 2 3 5
do
	scp /etc/hosts hadoop@group2worker$var.dyn.mwn.de:/etc/hosts
done

for var in 7 8
do
        scp /etc/hosts hadoop@worker$var:/etc/hosts
done
