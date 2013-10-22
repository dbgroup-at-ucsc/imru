java -Xmx512m -cp bin\
:dist/lib/*\
:lib/hyracks-ec2/lib/*\
:lib/spark-0.8.0/spark-assembly-0.8.0-incubating-hadoop1.0.4.jar\
:lib/scala-2.9.3/lib/*\
 $@
