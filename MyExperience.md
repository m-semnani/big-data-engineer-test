## HDP Sandbox

First I started to work with HDP docker image, but it was too large to download and my VPN disconnected periodically, and all the process started again.
Then I switeched to download HDP VM. and it was again too large. finally I downloaded it but because of disk space, I couldn't bring that up.
Finally I created my own VM and installed Hadoop and Hbase on that.

## HBase

Some of the tasks was related to Spark Sql which I Haven't worked a lot with. But I knew the concept and finish the assignment.
For storing data on HBase, I used to direct connections. But lately Hortonworks released a spark hbase connector which I found it 
very well suited here, and it was very well matched to spark sql.

## Kafka

There were two good choices for dockerized kafka. One was "wurstmeister/kafka" and the other was "spotify/kafka".
The first one uses docker compose and brings up images (zookeeper, kafka) in different containters. I played with that 
and found it a little hard to work with. So I started to work with the second one whiche brings everthing in one container.
The tricky part of running that was setting correct IP address and port to expose to the host.

About Streaming, I decided to use spark, because we used it in the previous part of the assignment and we better choose as less technology as possible
for maintenance reasons. I also choosed structured streaming instead of the previous streaming api, because it handles writing to kafka more cleaner than 
the previous one. Also it is newer and recommended one.
 

