---
layout: doc_page
---

# Booting a Druid Cluster
[Loading Your Data](Tutorial%3A-Loading-Your-Data-Part-2.html) and [All About Queries](Tutorial%3A-All-About-Queries.html) contain recipes to boot a small druid cluster on localhost. However, when it's time to run a more realistic setup&mdash;for production or just for testing production&mdash;you'll want to find a way to start the cluster on multiple hosts. This document describes two different ways to do this: manually, or as a cloud service via Apache Whirr.

## Manually Booting a Druid Cluster
You can provision individual servers, loading Druid onto each machine (or building it) and setting the required configuration for each type of node. You'll also have to set up required external dependencies. Then you'll have to start each node. This process is outlined in [Tutorial: The Druid Cluster](Tutorial:-The-Druid-Cluster.html).

## Apache Whirr

[Apache Whirr](http://whirr.apache.org/) is a set of libraries for launching cloud services. For Druid, Whirr serves as an easy way to launch a cluster in Amazon AWS by using simple commands and configuration files (called *recipes*).

**NOTE:** Whirr will install Druid 0.5.x. At this time Whirr can launch Druid 0.5.x only, but in the near future will support Druid 0.6.x. You can download and install your own copy of Druid 0.5.x [here](http://static.druid.io/artifacts/releases/druid-services-0.5.7-bin.tar.gz).

You'll need an AWS account, and an EC2 key pair from that account so that Whirr can connect to the cloud via the EC2 API. If you haven't generated a key pair, see the [AWS documentation](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html) or see this [Whirr FAQ](http://whirr.apache.org/faq.html#how-do-i-find-my-cloud-credentials).


### Installing Whirr
You must use a version of Whirr that includes and supports a Druid recipe. You can do it so in one of two ways:

#### Build the Following Version of Whirr
Clone the code from [https://github.com/rjurney/whirr/tree/trunk](https://github.com/rjurney/whirr/tree/trunk) and build Whirr:

    git clone git@github.com:rjurney/whirr.git
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
* You can choose the hardware used with `whirr.hardware-id` to a specific AWS region (e.g., m1.large). If you don't choose an image via `whirr.image-id` (image must be compatible with hardware), you'll get plain vanilla Linux.
* SSH keys (not password protected) must exist for the local user. If they are in the default locations, `${sys:user.home}/.ssh/id_rsa` and `${sys:user.home}/.ssh/id_rsa.pub`, Whirr will find them. Otherwise, you'll have to specify them with `whirr.private-key-file` and `whirr.public-key-file`.
* Be sure to specify the absolute path of the Druid realtime spec file `realtime.spec` in `whirr.druid.realtime.spec.path`.
* Two Druid cluster templates (see `whirr.instance-templates`) are provided: a small cluster running on a single EC2 instance, and a larger cluster running on multiple instances. The first is a good test case to start with.

The following AWS information must be set in `druid.properties`, as environment variables, or in the file `$WHIRR_HOME/conf/credentials`:

    PROVIDER=aws-ec2
    IDENTITY=<aws-id-key>
    CREDENTIAL=<aws-private-key>
    
How to get the IDENTITY and CREDENTIAL keys is discussed above.

### Start a Test Cluster With Whirr
Run the following command:

```bash
% $WHIRR_HOME/bin/whirr launch-cluster --config $WHIRR_HOME/recipes/druid.properties
```
If Whirr starts without an errors, you should see the following message:

    Running on provider aws-ec2 using identity <your-aws-id-here>
    
You can then use the EC2 dashboard to locate the instance and confirm that it has started up.

If both the instance and the Druid cluster launch successfully, a few minutes later other messages to STDOUT should follow with information returned from EC2, including the instance ID:

    Started cluster of 1 instances
    Cluster{instances=[Instance{roles=[zookeeper, druid-mysql, druid-coordinator, druid-broker, druid-historical, druid-realtime], publicIp= ...
    
The final message will contain login information for the instance.

Note that the Whirr will return an exception if any of the nodes fail to launch, and the cluster will be destroyed. To destroy the cluster manually, run the following command:

```bash
% $WHIRR_HOME/bin/whirr destroy-cluster --config $WHIRR_HOME/recipes/druid.properties
```


