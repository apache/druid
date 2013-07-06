# Before running, you will need to download the EC2 tools from http://aws.amazon.com/developertools/351
# and then setup your EC2_HOME and PATH variables (or similar):
# 
# # Setup environment for ec2-api-tools
# export EC2_HOME=/path/to/ec2-api-tools-1.6.7.4/
# export PATH=$PATH:$EC2_HOME/bin
# export AWS_ACCESS_KEY=
# export AWS_SECRET_KEY=

# Check for ec2 commands we require and die if they're missing
type ec2-create-keypair >/dev/null 2>&1 || { echo >&2 "I require ec2-create-keypair but it's not installed.  Aborting."; exit 1; }
type ec2-create-group >/dev/null 2>&1 || { echo >&2 "I require ec2-create-group but it's not installed.  Aborting."; exit 1; }
type ec2-authorize >/dev/null 2>&1 || { echo >&2 "I require ec2-authorize but it's not installed.  Aborting."; exit 1; }
type ec2-run-instances >/dev/null 2>&1 || { echo >&2 "I require ec2-run-instances but it's not installed.  Aborting."; exit 1; }
type ec2-describe-instances >/dev/null 2>&1 || { echo >&2 "I require ec2-describe-instances but it's not installed.  Aborting."; exit 1; }

# Create a keypair for our servers
echo "Removing old keypair for druid..."
ec2-delete-keypair druid-keypair
echo "Creating new keypair for druid..."
ec2-create-keypair druid-keypair > druid-keypair
chmod 0600 druid-keypair
mv druid-keypair ~/.ssh/

# Create a security group for our servers
echo "Creating a new security group for druid..."
ec2-create-group druid-group -d "Druid Cluster"

# Create rules that allow necessary services in our group
echo "Creating new firewall rules for druid..."
# SSH from outside
ec2-authorize druid-group -P tcp -p 22
# Enable all traffic within group
ec2-authorize druid-group -P tcp -p 1-65535 -o druid-group
ec2-authorize druid-group -P udp -p 1-65535 -o druid-group

echo "Booting a single small instance for druid..."
# Use ami ami-e7582d8e - Alestic Ubuntu 12.04 us-east
INSTANCE_ID=$(ec2-run-instances ami-e7582d8e -n 1 -g druid-group -k druid-keypair --instance-type m1.small| awk '/INSTANCE/{print $2}')
while true; do
    sleep 1
    INSTANCE_STATUS=$(ec2-describe-instances|grep INSTANCE|grep $INSTANCE_ID|cut -f6)
    if [ $INSTANCE_STATUS == "running" ]
        then
            echo "Instance $INSTANCE_ID is status $INSTANCE_STATUS..."
            break
    fi
done

# Wait for the instance to come up
echo "Waiting 60 seconds for instance $INSTANCE_ID to boot..."
sleep 60

# Get hostname and ssh with the key we created, and ssh there
INSTANCE_ADDRESS=`ec2-describe-instances|grep 'INSTANCE'|grep $INSTANCE_ID|cut -f4`
echo "Connecting to $INSTANCE_ADDRESS to prepare environment for druid..."
ssh -i ~/.ssh/druid-keypair -o StrictHostKeyChecking=no ubuntu@${INSTANCE_ADDRESS} <<'EOI'

  # Setup Oracle Java
  sudo apt-get purge openjdk*
  sudo rm /var/lib/dpkg/info/oracle-java7-installer*
  sudo apt-get purge oracle-java7-installer*
  sudo rm /etc/apt/sources.list.d/*java*
  sudo apt-get update
  sudo add-apt-repository -y ppa:webupd8team/java
  sudo apt-get update
  
  # Setup yes answer to license question
  echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
  echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections
  sudo apt-get -y install oracle-java7-installer
  
  # Automated Kafka setup
  curl http://static.druid.io/artifacts/kafka-0.7.2-incubating-bin.tar.gz -o /tmp/kafka-0.7.2-incubating-bin.tar.gz
  tar -xvzf /tmp/kafka-0.7.2-incubating-bin.tar.gz
  cd kafka-0.7.2-incubating-bin
  cat config/zookeeper.properties
  nohup bin/zookeeper-server-start.sh config/zookeeper.properties &
  # in a new console
  nohup bin/kafka-server-start.sh config/server.properties &
  
  # Install dependencies - mysql must be built from source, as the 12.04 apt-get hangs
  export DEBIAN_FRONTEND=noninteractive
  sudo debconf-set-selections <<< 'mysql-server-5.5 mysql-server/root_password password diurd'
  sudo debconf-set-selections <<< 'mysql-server-5.5 mysql-server/root_password_again password diurd'
  sudo apt-get -q -y -V --force-yes --reinstall install mysql-server-5.5
  
  echo "ALL DONE with druid environment setup! Hit CTRL-C to proceed."
  logout
EOI

echo "Prepared $INSTANCE_ADDRESS for druid."

# Now to scp a tarball up that can run druid!
if [ -f ../../services/target/druid-services-*-SNAPSHOT-bin.tar.gz ];
then
  echo "Uploading druid tarball to server..."
  scp -i ~/.ssh/druid-keypair -o StrictHostKeyChecking=no ../../services/target/druid-services-*-bin.tar.gz ubuntu@${INSTANCE_ADDRESS}:
else
  echo "ERROR - package not built!"
fi

# Now boot druid parts
ssh -i ~/.ssh/druid-keypair -o StrictHostKeyChecking=no ubuntu@${INSTANCE_ADDRESS} <<'EOI2'

  # Is localhost expected with multi-node?
  mysql -u root -pdiurd -e "GRANT ALL ON druid.* TO 'druid'@'localhost' IDENTIFIED BY 'diurd'; CREATE database druid;"

  tar -xvzf druid-services-*-bin.tar.gz
  cd druid-services-*
  
  mkdir logs
  
  # Now start a realtime node
  nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -Ddruid.realtime.specFile=config/realtime/realtime.spec -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/realtime com.metamx.druid.realtime.RealtimeMain 2>&1 > logs/realtime.log &

  # And a master node
  nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/master com.metamx.druid.http.MasterMain 2>&1 > logs/master.log &

  # And a compute node
  nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/compute com.metamx.druid.http.ComputeMain 2>&1 > logs/compute.log &

  # And a broker node
  nohup java -Xmx256m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -classpath lib/druid-services-0.5.5-SNAPSHOT-selfcontained.jar:config/broker com.metamx.druid.http.BrokerMain 2>&1 > logs/broker.log &
EOI2

echo "Druid booting complete!"
echo "ssh -i ~/.ssh/druid-keypair ubuntu@${INSTANCE_ADDRESS} #to connect"
