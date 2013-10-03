#WTF is this?? -- FJ

# Is localhost expected with multi-node?
mysql -u root -pdiurd -e "GRANT ALL ON druid.* TO 'druid'@'localhost' IDENTIFIED BY 'diurd'; CREATE database druid;" 2>&1 > /dev/null

tar -xvzf druid-services-*-bin.tar.gz 2>&1 > /dev/null
cd druid-services-* 2>&1 > /dev/null

mkdir logs 2>&1 > /dev/null

# Now start a realtime node
nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.realtime.specFile=config/realtime/realtime.spec -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/realtime io.druid.cli.Main server realtime 2>&1 > logs/realtime.log &

# And a coordinator node
nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/coordinator io.druid.cli.Main server coordinator 2>&1 > logs/coordinator.log &

# And a historical node
nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/historical io.druid.cli.Main server historical 2>&1 > logs/historical.log &

# And a broker node
nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/broker io.druid.cli.Main server broker 2>&1 > logs/broker.log &

echo "Hit CTRL-C to continue..."
exit 0
