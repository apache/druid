---
layout: doc_page
---
This page describes how to use Riak-CS for deep storage instead of S3.  We are still setting up some of the peripheral stuff (file downloads, etc.).

This guide provided by Pablo Nebrera, thanks!

## The VMWare instance

A VMWare [image](http://static.druid.io/artifacts/vmware/druid_riak.tgz) based on Druid 0.3.27.2 and built according to the instructions below has also been provided by Pablo Nebrera.

The provided vmware machine has access with the following credentials:

    username: root
    password: riakdruid


## The Setup

We started with a minimal CentOS installation but you can use any other compatible installation. At the end of this setup you will one node that is running:

1. A Kafka Broker
1. A single-node Zookeeper ensemble
1. A single-node Riak-CS cluster
1. A Druid [Coordinator](Coordinator.html)
1. A Druid [Broker](Broker.html)
1. A Druid [Historical](Historical.html)
1. A Druid [Realtime](Realtime.html)

This just walks through getting the relevant software installed and running.  You will then need to configure the [Realtime](Realtime.html) node to take in your data.

### Configure System

1. Install `CentOS-6.4-x86_64-minimal.iso` ("RedHat v6.4" is the name of the AWS AMI) or your favorite Linux OS (if you use a different OS, some of the installation instructions for peripheral services might differ, please adjust them according to the system you are using).  The rest of these instructions assume that you have a running instance and are running as the root user.

1. Configure the network. We used dhcp executing:
    
        dhclient eth0

1. Disable firewall for now

        service iptables stop
        chkconfig iptables off

1. Change the limits on the number of open files a process can have:

        cat >> /etc/security/limits.conf <<- _RBEOF_
        # ulimit settings for Riak CS
        root soft nofile 65536
        root hard nofile 65536
        riak soft nofile 65536
        riak hard nofile 65536
        _RBEOF_

        ulimit -n 65536

### Install base software packages

1. Install necessary software with yum

        yum install -y java-1.7.0-openjdk-devel git wget metadata storage-server

1. Install maven

        wget http://apache.rediris.es/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
        tar xzf apache-maven-3.0.5-bin.tar.gz -C /usr/local
        pushd /usr/local
        sudo ln -s apache-maven-3.0.5 maven
        popd
        echo 'export M2_HOME=/usr/local/maven' >> /etc/profile.d/maven.sh
        echo 'export PATH=${M2_HOME}/bin:${PATH}' >> /etc/profile.d/maven.sh
        source /etc/profile.d/maven.sh

1. Install erlang

        wget http://binaries.erlang-solutions.com/rpm/centos/6/x86_64/esl-erlang-R15B01-1.x86_64.rpm
        yum localinstall -y esl-erlang-R15B01-1.x86_64.rpm

### Install Kafka And Zookeeper

1. Install kafka and zookeeper:

        wget http://apache.rediris.es/incubator/kafka/kafka-0.7.2-incubating/kafka-0.7.2-incubating-src.tgz
        tar zxvf kafka-0.7.2-incubating-src.tgz
        pushd kafka-0.7.2-incubating-src/
        ./sbt update 
        ./sbt package
        mkdir -p /var/lib/kafka
        rsync -a * /var/lib/kafka/
        popd

### Install Riak-CS

1. Install s3cmd to manage riak s3

        wget http://downloads.sourceforge.net/project/s3tools/s3cmd/1.5.0-alpha3/s3cmd-1.5.0-alpha3.tar.gz
        tar xzvf s3cmd-1.5.0-alpha3.tar.gz
        cd s3cmd-1.5.0-alpha3
        cp -r s3cmd S3 /usr/local/bin/

1. Install riak, riak-cs and stanchion.  Note: riak-cs-control is optional

        wget http://s3.amazonaws.com/downloads.basho.com/riak/1.3/1.3.1/rhel/6/riak-1.3.1-1.el6.x86_64.rpm
        wget http://s3.amazonaws.com/downloads.basho.com/riak-cs/1.3/1.3.1/rhel/6/riak-cs-1.3.1-1.el6.x86_64.rpm
        wget http://s3.amazonaws.com/downloads.basho.com/stanchion/1.3/1.3.1/rhel/6/stanchion-1.3.1-1.el6.x86_64.rpm
        wget http://s3.amazonaws.com/downloads.basho.com/riak-cs-control/1.0/1.0.0/rhel/6/riak-cs-control-1.0.0-1.el6.x86_64.rpm
        yum localinstall -y riak-*.rpm stanchion-*.rpm

### Install Druid

1. Clone the git repository for druid, checkout a "stable" tag and build 

        git clone https://github.com/metamx/druid.git druid
        pushd druid
        git checkout druid-0.4.12
        export LANGUAGE=C
        export LC_MESSAGE=C
        export LC_ALL=C
        export LANG=en_US
        ./build.sh
        mkdir -p /var/lib/druid/app
        cp ./services/target/druid-services-*-selfcontained.jar /var/lib/druid/app
        ln -s /var/lib/druid/app/druid-services-*-selfcontained.jar /var/lib/druid/app/druid-services.jar
        popd


### Configure stuff

1. Add this line to /etc/hosts

        echo "127.0.0.1   s3.amazonaws.com bucket.s3.amazonaws.com `hostname`" >> /etc/hosts

    NOTE: the bucket name in this case is "bucket", but you might need to update it to your bucket name if you want to use a different bucket name.

1. Download and extract run scripts and configuration files: 

        wget http://static.druid.io/artifacts/scripts/druid_scripts_nebrera.tar /
        pushd /
        tar xvf ~/druid_scripts_nebrera.tar
        popd


1. Start Riak in order to create a user:

        /etc/init.d/riak start 
        /etc/init.d/riak-cs start
        /etc/init.d/stanchion start

    You can check riak status using:

        riak-admin member-status
        
    You should expect results like

        Attempting to restart script through sudo -H -u riak
        ================================= Membership ==================================
        Status     Ring    Pending    Node
        -------------------------------------------------------------------------------
        valid     100.0%      --      'riak@127.0.0.1'
        -------------------------------------------------------------------------------
        Valid:1 / Leaving:0 / Exiting:0 / Joining:0 / Down:0


1. Create riak-cs user and yoink out credentials.

        curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:8088/riak-cs/user --data '{"email":"example@domain.com", "name":"admin"}' >> /tmp/riak_user.json
        export RIAK_KEY_ID=`sed 's/^.*"key_id":"//' /tmp/riak_user.json | cut -d '"' -f 1`
        export RIAK_KEY_SECRET=`sed 's/^.*"key_secret":"//' /tmp/riak_user.json | cut -d '"' -f 1`
        sed -i "s/<%=[ ]*@key_id[ ]*%>/${RIAK_KEY_ID}/" /etc/riak-cs/app.config /etc/riak-cs-control/app.config /etc/stanchion/app.config /etc/druid/config.sh /etc/druid/base.properties /root/.s3cfg
        sed -i "s/<%=[ ]*@key_secret[ ]*%>/${RIAK_KEY_SECRET}/" /etc/riak-cs/app.config /etc/riak-cs-control/app.config /etc/stanchion/app.config /etc/druid/config.sh /etc/druid/base.properties /root/.s3cfg

    This will store the result of creating the user into `/tmp/riak_user.json`.  You can look at it if you are interested.  It will look something like this

        {"email":"example@domain.com",
         "display_name":"example",
         "name":"admin",
         "key_id":"DOXKZYR_QM2S-7HSKAEU",
         "key_secret":"GtvVJow068RM-_viHIYR9DWMAXsFcL1SmjuNfA==",
         "id":"4c5b5468c180f3efafd531b6cd8e2bb24371d99640aad5ced5fbbc0604fc473d",
         "status":"enabled"} 

1. Stop riak-cs:

        /etc/init.d/riak-cs stop
        /etc/init.d/stanchion stop
        /etc/init.d/riak stop

1. Disable anonymous user creation

        sed 's/{[ ]*anonymous_user_creation[ ]*,[ ]*true[ ]*}/{anonymous_user_creation, false}/' /etc/riak-cs/app.config |grep anonymous_user_creation

1. Restart riak-cs services:

        /etc/init.d/riak start
        /etc/init.d/riak-cs start
        /etc/init.d/stanchion start


1. Create your bucket. The example name and in config files is "bucket"

        s3cmd mb s3://bucket

    You can verify that the bucket is created with:

        s3cmd ls

1. Start metadata storage server

        service metadata storaged start
        chkconfig metadata storaged on
        /usr/bin/metadata storageadmin -u root password 'riakdruid'

    NOTE: If you don't like "riakdruid" as your password, feel free to change it around.
    NOTE: If you have used root user to connect to database. It should be changed by other user but I have used this one to simplify it

1. Start zookeeper and kafka

        /etc/init.d/zookeeper start
        /etc/init.d/kafka start

1. Start druid

        /etc/init.d/druid_master start
        /etc/init.d/druid_realtime start
        /etc/init.d/druid_broker start
        /etc/init.d/druid_compute start
