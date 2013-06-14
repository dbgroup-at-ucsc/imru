java -Xmx512m -cp bin\
:dist/lib/*\
:lib/hyracks-ec2/lib/*\
:lib/spark-0.7.0/spark-core_2.9.2-0.7.0.jar\
:lib/scala-2.9.2/lib/*\
:lib/spark-0.7.0/lib_managed/bundles/*\
:lib/spark-0.7.0/lib_managed/jars/*\
 $@
