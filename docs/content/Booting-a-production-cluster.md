---
layout: doc_page
---

# Booting a Druid Cluster
[Loading Your Data](Tutorial%3A-Loading-Your-Data-Part-2.html) and [All About Queries](Tutorial%3A-All-About-Queries.html) contain recipes to boot a small druid cluster on localhost. However, when it's time to run a more realistic setup, for production or just for testing production, you'll want to find a way to start the cluster on multiple hosts. This document describes two different ways to do this: manually, or as a cloud service via Apache Whirr.

## Manually Booting a Druid Cluster
You can provision individual servers, loading Druid onto each machine (or building it) and setting the required configuration for each type of node. You'll also have to set up required external dependencies. Then you'll have to start each node. this process is outlined in [Tutorial: The Druid Cluster](Tutorial:-The-Druid-Cluster.html).

## Apache Whirr

[Apache Whirr](http://whirr.apache.org/) is a set of libraries for launching cloud services. For Druid, Whirr serves as an easy way to launch a cluster in Amazon AWS by using simple commands and configuration files (called *recipes*).

You'll need an AWS account, and an EC2 key pair from that account so that Whirr can connect to it. If you haven't generated a key pair, see the [AWS documentation](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html) or see this [Whirr FAQ](http://whirr.apache.org/faq.html#how-do-i-find-my-cloud-credentials).

### Installing Whirr
To get a version of Whirr that includes and supports a Druid recipe, clone the code from [https://github.com/rjurney/whirr/tree/trunk](https://github.com/rjurney/whirr/tree/trunk) and build Whirr:

    git clone git@github.com:rjurney/whirr.git
    cd whirr
    git checkout trunk
    mvn clean install -Dmaven.test.failure.ignore=true

### 