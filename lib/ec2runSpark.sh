java -Xmx512m -cp bin\
:/home/ubuntu/hyracks-ec2/lib/*\
:/home/ubuntu/spark-0.7.0/spark-core_2.9.2-0.7.0.jar\
:/home/ubuntu/spark-0.7.0/core/target/scala-2.9.2/classes\
:/home/ubuntu/spark-0.7.0/core/target/scala-2.9.2/spark-core_2.9.2-0.7.0.jar\
:/home/ubuntu/scala-2.9.2/lib/*\
:/home/ubuntu/spark-0.7.0/lib_managed/bundles/*\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/*\
 $@
