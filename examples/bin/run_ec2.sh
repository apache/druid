# Before running, you will need to download the EC2 tools from http://aws.amazon.com/developertools/351
# and then setup your EC2_HOME and PATH variables (or similar):
# 
# unzip ec2-api-tools.zip
# cd ec2-api-tools-*
# 
# # Setup environment for ec2-api-tools
# export EC2_HOME=`pwd`
# echo "EC2_HOME=`pwd`" >> ~/.bash_profile
# echo 'export PATH=$PATH:$EC2_HOME/bin' >> ~/.bash_profile
# source ~/.bash_profile

AWS_ACCESS_KEY=
AWS_SECRET_KEY=

# Create a keypair for our servers
ec2-create-keypair druid-keypair > druid-keypair
chmod 0600 druid-keypair
mv druid-keypair ~/.ssh/

# Create a security group for our servers
ec2-create-group druid-group -d "Druid Cluster"

# Create rules that allow necessary services in our group
# SSH from outside
ec2-authorize druid-group -P tcp -p 22
# Enable all traffic within group
ec2-authorize druid-group -P tcp -p 1-65535 -o druid-group
ec2-authorize druid-group -P udp -p 1-65535 -o druid-group

# Use ami ami-e7582d8e - Alestic Ubuntu 12.04 us-east
ec2-run-instances ami-e7582d8e -n 1 -g druid-group -k druid-keypair --instance-type m1.small
ec2-describe-instances

# Get hostname and ssh with the key we created, and ssh there
INSTANCE_ADDRESS=`ec2-describe-instances|grep 'INSTANCE'|cut -f4` && echo $INSTANCE_ADDRESS
ssh -i ~/.ssh/druid-keypair -o StrictHostKeyChecking=no ubuntu@${INSTANCE_ADDRESS} <<\EOI
  # Install dependencies - mysql and Java, and setup druid db
  export DEBIAN_FRONTEND=noninteractive
  sudo apt-get -q -y install mysql-server

  # Is localhost expected with multi-node?
  mysql -u root -e "GRANT ALL ON druid.* TO 'druid'@'localhost' IDENTIFIED BY 'diurd'; CREATE database druid;"

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
  
  # Logout
  exit
EOI

echo "Prepared $INSTANCE_ADDRESS for druid."
