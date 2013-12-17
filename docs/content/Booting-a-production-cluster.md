---
layout: doc_page
---
# Booting a Single Node Cluster #

[Loading Your Data](Tutorial%3A-Loading-Your-Data-Part-2.html) and [All About Queries](Tutorial%3A-All-About-Queries.html) contain recipes to boot a small druid cluster on localhost. Here we will boot a small cluster on EC2. You can checkout the code, or download a tarball from [here](http://static.druid.io/artifacts/druid-services-0.6.37-bin.tar.gz).

The [ec2 run script](https://github.com/metamx/druid/blob/master/examples/bin/run_ec2.sh), run_ec2.sh, is located at 'examples/bin' if you have checked out the code, or at the root of the project if you've downloaded a tarball. The scripts rely on the [Amazon EC2 API Tools](http://aws.amazon.com/developertools/351), and you will need to set three environment variables:

```bash
# Setup environment for ec2-api-tools
export EC2_HOME=/path/to/ec2-api-tools-1.6.7.4/
export PATH=$PATH:$EC2_HOME/bin
export AWS_ACCESS_KEY=
export AWS_SECRET_KEY=
```

Then, booting an ec2 instance running one node of each type is as simple as running the script, run_ec2.sh :)

# Apache Whirr #

Apache Whirr is a set of libraries for launching cloud services. You can clone a version of Whirr that includes Druid as a service from git@github.com:rjurney/whirr.git:

```bash
git clone git@github.com:rjurney/whirr.git
cd whirr
git checkout trunk
mvn clean install -Dmaven.test.failure.ignore=true -Dcheckstyle.skip
sp;bin/whirr launch-cluster --config recipes/druid.properties
```
