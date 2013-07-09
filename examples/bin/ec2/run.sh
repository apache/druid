# Is localhost expected with multi-node?
mysql -u root -pdiurd -e "GRANT ALL ON druid.* TO 'druid'@'localhost' IDENTIFIED BY 'diurd'; CREATE database druid;" 2>&1 > /dev/null

tar -xvzf druid-services-*-bin.tar.gz 2>&1 > /dev/null
cd druid-services-* 2>&1 > /dev/null

mkdir logs 2>&1 > /dev/null

# Now start a realtime node
nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.realtime.specFile=config/realtime/realtime.spec -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/realtime com.metamx.druid.realtime.RealtimeMain 2>&1 > logs/realtime.log &

# And a master node
nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/master com.metamx.druid.http.MasterMain 2>&1 > logs/master.log &

# And a compute node
nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/compute com.metamx.druid.http.ComputeMain 2>&1 > logs/compute.log &

# And a broker node
nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/broker com.metamx.druid.http.BrokerMain 2>&1 > logs/broker.log &

echo "Hit CTRL-C to continue..."
exit 0
