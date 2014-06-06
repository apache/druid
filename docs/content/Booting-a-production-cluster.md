---
layout: doc_page
---

# Booting a Druid Cluster
[Loading Your Data](Tutorial%3A-Loading-Your-Data-Part-2.html) and [All About Queries](Tutorial%3A-All-About-Queries.html) contain recipes to boot a small druid cluster on localhost. However, when it's time to run a more realistic setup&mdash;for production or just for testing production&mdash;you'll want to find a way to start the cluster on multiple hosts. This document describes two different ways to do this: manually, or as a cloud service via Apache Whirr.

## Manually Booting a Druid Cluster
You can provision individual servers, loading Druid onto each machine (or building it) and setting the required configuration for each type of node. You'll also have to set up required external dependencies. Then you'll have to start each node. This process is outlined in [Tutorial: The Druid Cluster](Tutorial:-The-Druid-Cluster.html).

## Apache Whirr

[Apache Whirr](http://whirr.apache.org/) is a set of libraries for launching cloud services. For Druid, Whirr serves as an easy way to launch a cluster in Amazon AWS by using simple commands and configuration files (called *recipes*).

**NOTE:** Whirr will install Druid 0.6.121. Also, it doesn't work with JDK1.7.0_55. JDK1.7.0_45 recommended.

You'll need an AWS account, S3 Bucket and an EC2 key pair from that account so that Whirr can connect to the cloud via the EC2 API. If you haven't generated a key pair, see the [AWS documentation](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html) or see this [Whirr FAQ](http://whirr.apache.org/faq.html#how-do-i-find-my-cloud-credentials).


### Installing Whirr
You must use a version of Whirr that includes and supports a Druid recipe. You can do it so in one of two ways:

#### Build the Following Version of Whirr
Clone the code from [https://github.com/druid-io/whirr](https://github.com/druid-io/whirr) and build Whirr:

    git clone git@github.com:druid-io/whirr.git
    cd whirr
    git checkout trunk
    mvn clean install -Dmaven.test.failure.ignore=true

#### Build the Latest Version of Whirr
Clone the code from the Whirr repository:

    git clone git://git.apache.org/whirr.git
    
Then run `mvn install` from the root directory.

### Configure Whirr
The Whirr recipe for Druid is the configuration file `$WHIRR_HOME/recipies/druid.properties`. You can edit this file to suit your needs -- it is annotated and self-explanatory. Here are some hints about that file:

* Set `whirr.location-id` to a specific AWS region (e.g., us-east-1) if desired, else one will be chosen for you.
* You can choose the hardware used with `whirr.hardware-id` to a specific instance type (e.g., m1.large). By default druid.properties, m3.2xlarge (broker, historical, middle manager), m1.xlarge (coordinator, overlord), and m1.small (zookeeper, mysql) are used.
* If you don't choose an image via `whirr.image-id` (image must be compatible with hardware), you'll get plain vanilla Linux. Default druid.properties uses ami-018c9568 (Ubuntu 12.04).
* SSH keys (not password protected) must exist for the local user. If they are in the default locations, `${sys:user.home}/.ssh/id_rsa` and `${sys:user.home}/.ssh/id_rsa.pub`, Whirr will find them. Otherwise, you'll have to specify them with `whirr.private-key-file` and `whirr.public-key-file`.
* Be sure to specify the absolute path of the Druid realtime spec file `realtime.spec` in `whirr.druid.realtime.spec.path`.
* Also make sure to specify the correct S3 bucket. Otherwise the cluster won't be able to process tasks.
* Two Druid cluster templates (see `whirr.instance-templates`) are provided: a small cluster running on a single EC2 instance, and a larger cluster running on multiple instances.

The following AWS information must be set in `druid.properties`, as environment variables, or in the file `$WHIRR_HOME/conf/credentials`:

    PROVIDER=aws-ec2
    IDENTITY=<aws-id-key>
    CREDENTIAL=<aws-private-key>
    
How to get the IDENTITY and CREDENTIAL keys is discussed above.

In order to configure each node, you can edit `services/druid/src/main/resources/functions/start_druid.sh` for JVM configuration and `services/druid/src/main/resources/functions/configure_[NODE_NAME].sh` for specific node configuration. For more information on configuration, read the Druid documentations about it (http://druid.io/docs/0.6.116/Configuration.html).

### Start a Test Cluster With Whirr
Run the following command:

```bash
% $WHIRR_HOME/bin/whirr launch-cluster --config $WHIRR_HOME/recipes/druid.properties
```
If Whirr starts without any errors, you should see the following message:

    Running on provider aws-ec2 using identity <your-aws-id-here>
    
You can then use the EC2 dashboard to locate the instance and confirm that it has started up.

If both the instance and the Druid cluster launch successfully, a few minutes later other messages to STDOUT should follow with information returned from EC2, including the instance ID:

    Started cluster of 1 instances
    Cluster{instances=[Instance{roles=[zookeeper, druid-mysql, druid-coordinator, druid-broker, druid-historical, druid-realtime], publicIp= ...
    
The final message will contain login information for the instance.

Note that Whirr will return an exception if any of the nodes fail to launch, and the cluster will be destroyed. To destroy the cluster manually, run the following command:

```bash
% $WHIRR_HOME/bin/whirr destroy-cluster --config $WHIRR_HOME/recipes/druid.properties
```

### Testing the Cluster
Now you can run an indexing task and a simple query to see if all the nodes have launched correctly. We are going to use a Wikipedia example again. For a realtime indexing task, run the following command:

```bash
curl -X 'POST' -H 'Content-Type:application/json' -d @#{YOUR_DRUID_DIRECTORY}/examples/indexing/wikipedia_realtime_task.json #{OVERLORD_PUBLIC_IP_ADDR}:#{PORT}/druid/indexer/v1/task
```
Issuing the request should return a task ID.

To check the state of the overlord, open up your browser and go to `#{OVERLORD_PUBLIC_IP_ADDR}:#{PORT}/console.html`.

Next, go to `#{COORDINATOR_PUBLIC_IP_ADDR}:#{PORT}`. Click "View Information about the Cluster"->"Full Cluster View." You should now see the information about servers and segments. If the cluster runs correctly, Segment dimensions and Segment binaryVersion fields should be filled up. Allow few minutes for the segments to be processed.

Now you should be able to query the data using broker's public IP address.