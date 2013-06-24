rsync -vrultzCc --exclude="ec2.properties" /home/wangrui/ucscImru/bin/ labVM:/home/wangrui/ucscImru/bin/
#rsync -vrultzCc --exclude="ec2.properties" /home/wangrui/ucscImru/run.sh labVM:/home/wangrui/ucscImru/run.sh
rsync -vrultzCc --exclude="ec2.properties" labVM:/home/wangrui/ucscImru/result/ /home/wangrui/ucscImru/result/
