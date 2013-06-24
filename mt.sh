sudo umount /data/a/imru/ucscImru/src/java/edu/uci/ics/hyracks/imru/example
sudo umount /data/a/imru/ucscImru/src/resource
sudo umount /data/a/imru/ucscImru/src/java
sudo umount /data/a/imru/ucscImru/data
sudo umount /data/a/imru/ucscImru/dist
sudo mount --bind /data/a/imru/hyracks/imru/imru-core/src/main/java /data/a/imru/ucscImru/src/java
sudo mount --bind /data/a/imru/hyracks/imru/imru-core/src/main/resources /data/a/imru/ucscImru/src/resource
sudo mount --bind /data/a/imru/hyracks/imru/imru-example/src/main/java/edu/uci/ics/hyracks/imru/example /data/a/imru/ucscImru/src/java/edu/uci/ics/hyracks/imru/example
sudo mount --bind /data/a/imru/hyracks/imru/imru-example/data /data/a/imru/ucscImru/data
sudo mount --bind /data/a/imru/hyracks/imru/imru-dist/target/appassembler /data/a/imru/ucscImru/dist
sudo mount -t vboxsf data /data/data -o uid=1000,gid=1000
#sudo mount -t vboxsf verilog /data/a/otherCode/verilog -o uid=1000,gid=1000


