java -Xmx512m -cp bin\
:/home/ubuntu/hyracks-ec2/lib/ant-1.6.5.jar\
:/home/ubuntu/hyracks-ec2/lib/args4j-2.0.12.jar\
:/home/ubuntu/hyracks-ec2/lib/aws-java-sdk-1.3.27.jar\
:/home/ubuntu/hyracks-ec2/lib/commons-cli-1.2.jar\
:/home/ubuntu/hyracks-ec2/lib/commons-codec-1.4.jar\
:/home/ubuntu/hyracks-ec2/lib/commons-el-1.0.jar\
:/home/ubuntu/hyracks-ec2/lib/commons-httpclient-3.0.1.jar\
:/home/ubuntu/hyracks-ec2/lib/commons-io-1.4.jar\
:/home/ubuntu/hyracks-ec2/lib/commons-lang3-3.1.jar\
:/home/ubuntu/hyracks-ec2/lib/commons-logging-1.1.1.jar\
:/home/ubuntu/hyracks-ec2/lib/commons-net-1.4.1.jar\
:/home/ubuntu/hyracks-ec2/lib/core-3.1.1.jar\
:/home/ubuntu/hyracks-ec2/lib/dcache-client-0.0.1.jar\
:/home/ubuntu/hyracks-ec2/lib/ftplet-api-1.0.0.jar\
:/home/ubuntu/hyracks-ec2/lib/ftpserver-core-1.0.0.jar\
:/home/ubuntu/hyracks-ec2/lib/ftpserver-deprecated-1.0.0-M2.jar\
:/home/ubuntu/hyracks-ec2/lib/hadoop-core-0.20.2.jar\
:/home/ubuntu/hyracks-ec2/lib/hadoop-test-0.20.2.jar\
:/home/ubuntu/hyracks-ec2/lib/hsqldb-1.8.0.10.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-api-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-control-cc-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-control-common-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-control-nc-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-data-std-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-dataflow-common-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-dataflow-std-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-ec2-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-hdfs-0.20.2-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-hdfs-core-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-ipc-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-net-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-server-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-storage-am-btree-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-storage-am-common-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/hyracks-storage-common-0.2.3-SNAPSHOT.jar\
:/home/ubuntu/hyracks-ec2/lib/jackson-core-asl-1.8.9.jar\
:/home/ubuntu/hyracks-ec2/lib/jackson-mapper-asl-1.8.9.jar\
:/home/ubuntu/hyracks-ec2/lib/jasper-compiler-5.5.12.jar\
:/home/ubuntu/hyracks-ec2/lib/jasper-runtime-5.5.12.jar\
:/home/ubuntu/hyracks-ec2/lib/javax.servlet-api-3.0.1.jar\
:/home/ubuntu/hyracks-ec2/lib/jets3t-0.7.1.jar\
:/home/ubuntu/hyracks-ec2/lib/jetty-client-8.0.0.M0.jar\
:/home/ubuntu/hyracks-ec2/lib/jetty-continuation-8.0.0.RC0.jar\
:/home/ubuntu/hyracks-ec2/lib/jetty-http-8.0.0.RC0.jar\
:/home/ubuntu/hyracks-ec2/lib/jetty-io-8.0.0.RC0.jar\
:/home/ubuntu/hyracks-ec2/lib/jetty-security-8.0.0.RC0.jar\
:/home/ubuntu/hyracks-ec2/lib/jetty-server-8.0.0.RC0.jar\
:/home/ubuntu/hyracks-ec2/lib/jetty-servlet-8.0.0.RC0.jar\
:/home/ubuntu/hyracks-ec2/lib/jetty-util-8.0.0.RC0.jar\
:/home/ubuntu/hyracks-ec2/lib/jetty-webapp-8.0.0.RC0.jar\
:/home/ubuntu/hyracks-ec2/lib/jetty-xml-8.0.0.RC0.jar\
:/home/ubuntu/hyracks-ec2/lib/jsch-0.1.49.jar\
:/home/ubuntu/hyracks-ec2/lib/json-20090211.jar\
:/home/ubuntu/hyracks-ec2/lib/jsp-2.1-6.1.14.jar\
:/home/ubuntu/hyracks-ec2/lib/jsp-api-2.1-6.1.14.jar\
:/home/ubuntu/hyracks-ec2/lib/junit-4.8.1.jar\
:/home/ubuntu/hyracks-ec2/lib/kfs-0.3.jar\
:/home/ubuntu/hyracks-ec2/lib/mina-core-2.0.0-M5.jar\
:/home/ubuntu/hyracks-ec2/lib/oro-2.0.8.jar\
:/home/ubuntu/hyracks-ec2/lib/servlet-api-2.5-6.1.14.jar\
:/home/ubuntu/hyracks-ec2/lib/servlet-api-3.0.20100224.jar\
:/home/ubuntu/hyracks-ec2/lib/slf4j-api-1.6.1.jar\
:/home/ubuntu/hyracks-ec2/lib/slf4j-jcl-1.6.3.jar\
:/home/ubuntu/hyracks-ec2/lib/wicket-core-1.5.2.jar\
:/home/ubuntu/hyracks-ec2/lib/wicket-request-1.5.2.jar\
:/home/ubuntu/hyracks-ec2/lib/wicket-util-1.5.2.jar\
:/home/ubuntu/hyracks-ec2/lib/xmlenc-0.52.jar\
:/home/ubuntu/scala-2.9.2/lib/jline.jar\
:/home/ubuntu/scala-2.9.2/lib/scala-compiler.jar\
:/home/ubuntu/scala-2.9.2/lib/scala-dbc.jar\
:/home/ubuntu/scala-2.9.2/lib/scala-library.jar\
:/home/ubuntu/scala-2.9.2/lib/scala-partest.jar\
:/home/ubuntu/scala-2.9.2/lib/scala-swing.jar\
:/home/ubuntu/scala-2.9.2/lib/scalacheck.jar\
:/home/ubuntu/scala-2.9.2/lib/scalap.jar\
:/home/ubuntu/spark-0.7.0/core/target/scala-2.9.2/spark-core_2.9.2-0.7.0.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/bundles/compress-lzf-0.8.4.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/bundles/config-0.3.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/bundles/log4j-1.2.16.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/bundles/netty-3.2.7.Final.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/bundles/netty-3.5.3.Final.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/bundles/snappy-java-1.0.4.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/akka-actor-2.0.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/akka-remote-2.0.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/akka-slf4j-2.0.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/akka-zeromq-2.0.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/algebird-core_2.9.2-0.1.8.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/ant-1.6.5.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/asm-all-3.3.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/avro-1.6.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/avro-ipc-1.6.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/cglib-nodep-2.2.2.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/colt-1.2.0.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-beanutils-1.7.0.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-beanutils-core-1.8.0.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-cli-1.2.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-codec-1.4.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-collections-3.2.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-configuration-1.6.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-digester-1.8.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-el-1.0.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-httpclient-3.0.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-io-1.4.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-io-2.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-lang-2.6.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-logging-1.1.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-math-2.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/commons-net-1.4.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/concurrent-1.3.4.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/concurrentlinkedhashmap-lru-1.2.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/core-3.1.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/dispatch-json_2.9.1-0.8.5.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/easymock-3.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/fastutil-6.4.4.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/flume-ng-sdk-1.2.0.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/guava-11.0.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/h2-lzf-1.0.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/hadoop-core-1.0.4.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/hsqldb-1.8.0.10.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jackson-core-asl-1.0.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jackson-core-asl-1.9.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jackson-mapper-asl-1.0.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jackson-mapper-asl-1.9.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jasper-compiler-5.5.12.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jasper-runtime-5.5.12.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/JavaEWAH-0.6.6.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jets3t-0.7.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jline-0.9.94.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jna-3.0.9.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jnr-constants-0.8.2.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jsp-2.1-6.1.14.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jsp-api-2.1-6.1.14.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/jsr305-1.3.9.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/junit-3.8.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/junit-4.8.2.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/junit-interface-0.8.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/kfs-0.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/kryo-2.20-shaded.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/kryo-serializers-0.20.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/mesos-0.9.0-incubating.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/mimepull-1.6.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/objenesis-1.2.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/oro-2.0.8.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/paranamer-2.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/parboiled-core-1.0.2.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/parboiled-scala-1.0.2.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/protobuf-java-2.4.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/scalacheck_2.9.2-1.9.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/scalatest_2.9.2-1.8.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/servlet-api-2.5-20081211.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/servlet-api-2.5-20110124.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/servlet-api-2.5-6.1.14.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/servlet-api-2.5.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/sjson_2.9.1-0.15.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/slf4j-api-1.6.4.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/slf4j-log4j12-1.6.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/spray-base-1.0-M2.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/spray-can-1.0-M2.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/spray-io-1.0-M2.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/spray-json_2.9.2-1.1.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/spray-server-1.0-M2.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/spray-util-1.0-M2.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/test-interface-0.5.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/twirl-api_2.9.2-0.5.2.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/twitter4j-core-3.0.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/twitter4j-stream-3.0.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/velocity-1.7.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/xmlenc-0.52.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/zeromq-scala-binding_2.9.1-0.0.6.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/zkclient-0.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/zookeeper-3.3.3.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/httpclient-4.1.jar\
:/home/ubuntu/spark-0.7.0/lib_managed/jars/httpcore-4.1.jar\
 $1 $2 $3 $4