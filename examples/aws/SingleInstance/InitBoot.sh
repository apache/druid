#!/bin/sh
# Helper function

dnsName=$(curl -s http://169.254.169.254/latest/meta-data/public-hostname)

sudo sed -i "s/ec2-54-235-25-238.compute-1.amazonaws.com/${dnsName}/g" /etc/mysql/my.cnf

sudo service mysql restart

cd /usr/share/zookeeper/bin

sudo ./zkServer.sh start

sudo sed -i "s/ec2-54-235-25-238.compute-1.amazonaws.com/${dnsName}/g" /home/ubuntu/index.html

cd /home/ubuntu/druid/druid-services

sudo sed -i "s/ec2-54-235-25-238.compute-1.amazonaws.com/${dnsName}/g" /home/ubuntu/druid/druid-services/master/runtime.properties

sudo sed -i "s/ec2-54-235-25-238.compute-1.amazonaws.com/${dnsName}/g" /home/ubuntu/druid/druid-services/broker/runtime.properties

sudo sed -i "s/druid.host=ec2-54-235-25-238.compute-1.amazonaws.com/druid.host=${dnsName}/g" /home/ubuntu/druid/druid-services/compute/runtime.properties

sudo sed -i "s/ec2-54-235-25-238.compute-1.amazonaws.com/${dnsName}/g" /home/ubuntu/druid/druid-services/compute/runtime.properties

cd /home/ubuntu/druid/druid-services

sudo nohup java -Duser.timezone=UTC -Dfile.encoding=UTF-8 -cp master:druid-services-0.3.27.2-selfcontained.jar com.metamx.druid.http.MasterMain > nohup_masterLog 2>&1 &

sudo nohup java -Duser.timezone=UTC -Dfile.encoding=UTF-8 -cp broker:DruidSkilledAnalystsBroker.jar com.skilledanalysts.druid.demo.SkilledAnalystsDemoMain > nohup_skilledAnalystsDemoLog 2>&1 &

sudo nohup java -Duser.timezone=UTC -Dfile.encoding=UTF-8 -cp compute:druid-services-0.3.27.2-selfcontained.jar com.metamx.druid.http.ComputeMain > nohup_computeLog 2>&1 &


