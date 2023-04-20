hdfs dfs -cat /outputs/hadoop_python_wordcount/* | sort -rn -k2 | tail -n +101 | head -n 25

