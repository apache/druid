---
layout: doc_page
---

Example Production Hadoop Configuration
=======================================

The following configuration should work relatively well for Druid indexing and Hadoop. In the example, we are using Hadoop 2.4 with EC2 m1.xlarge nodes for NameNodes and cc2.8xlarge nodes for DataNodes.

### Core-site.xml

```
<configuration>

  <!-- Temporary directory on HDFS (but also sometimes local!) -->
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/mnt/persistent/hadoop</value>
  </property>
  
  <!-- S3 -->
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://#{IP}:9000</value>
  </property>
  <property>
    <name>fs.s3.impl</name>
    <value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value>
  </property>
  <property>
    <name>fs.s3.awsAccessKeyId</name>
    <value>#{S3_ACCESS_KEY}</value>
  </property>
  <property>
    <name>fs.s3.awsSecretAccessKey</name>
    <value>#{S3_SECRET_KEY}</value>
  </property>
  <property>
    <name>fs.s3.buffer.dir</name>
    <value>/mnt/persistent/hadoop-s3n</value>
  </property>
  <property>
    <name>fs.s3n.awsAccessKeyId</name>
    <value>#{S3N_ACCESS_KEY}</value>
  </property>
  <property>
    <name>fs.s3n.awsSecretAccessKey</name>
    <value>#{S3N_SECRET_KEY}</value>
  </property>

  <!-- Compression -->
  <property>
    <name>io.compression.codecs</name>
    <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.SnappyCodec</value>
  </property>

  <!-- JBOD -->
  <property>
    <name>io.seqfile.local.dir</name>
    <value>/mnt/persistent/hadoop/io/local</value>
  </property>

</configuration>
```

### Mapred-site.xml

```
<configuration>

  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>

  <property>
    <name>mapreduce.jobtracker.address</name>
    <value>#{JT_ADDR}:9001</value>
  </property>
  <property>
    <name>mapreduce.jobtracker.http.address</name>
    <value>#{JT_HTTP_ADDR}:9100</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>#{JH_ADDR}:10020</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>#{JH_WEBAPP_ADDR}:19888</value>
  </property>
  <property>
    <name>mapreduce.tasktracker.http.address</name>
    <value>#{TT_ADDR}:9103</value>
  </property>

  <!-- Memory and concurrency tuning -->
  <property>
    <name>mapreduce.job.reduces</name>
    <value>21</value>
  </property>
  <property>
  <property>
    <name>mapreduce.job.jvm.numtasks</name>
    <value>20</value>
  </property>
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>2048</value>
  </property>
  <property>
    <name>mapreduce.map.java.opts</name>
    <value>-server -Xmx1536m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>6144</value>
  </property>
  <property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-server -Xmx2560m -Duser.timezone=UTC -Dfile.encoding=UTF-8 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps</value>
  </property>
  <property>
    <name>mapreduce.reduce.shuffle.parallelcopies</name>
    <value>50</value>
  </property>
  <property>
    <name>mapreduce.reduce.shuffle.input.buffer.percent</name>
    <value>0.5</value>
  </property>
  <property>
    <name>mapreduce.task.io.sort.mb</name>
    <value>256</value>
  </property>
  <property>
    <name>mapreduce.task.io.sort.factor</name>
    <value>100</value>
  </property>
  <property>
    <name>mapreduce.jobtracker.handler.count</name>
    <value>64</value>
  </property>
  <property>
    <name>mapreduce.tasktracker.http.threads</name>
    <value>20</value>
  </property>
  
  <!-- JBOD -->
  <property>
    <name>mapreduce.cluster.local.dir</name>
    <value>/mnt/persistent/hadoop/mapred/local</value>
  </property>

  <!-- Job history server persistent state -->
  <property>
    <name>mapreduce.jobhistory.recovery.enable</name>
    <value>true</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.recovery.store.class</name>
    <value>org.apache.hadoop.mapreduce.v2.hs.HistoryServerFileSystemStateStoreService</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.recovery.store.fs.uri</name>
    <value>file://${hadoop.tmp.dir}/mapred-jobhistory-state</value>
  </property>

  <!-- Compression -->
  <property>
    <!-- Off by default, because it breaks Druid indexing (at least, it does it druid-0.6.10+). Jobs should turn
         it on if they need it. -->
    <name>mapreduce.output.fileoutputformat.compress</name>
    <value>false</value>
  </property>
  <property>
    <name>mapreduce.map.output.compress</name>
    <value>true</value>
  </property>
  <property>
    <name>mapreduce.output.fileoutputformat.compress.type</name>
    <value>BLOCK</value>
  </property>
  <property>
    <name>mapreduce.map.output.compress.codec</name>
    <value>org.apache.hadoop.io.compress.Lz4Codec</value>
  </property>
  <property>
    <name>mapreduce.output.fileoutputformat.compress.codec</name>
    <value>org.apache.hadoop.io.compress.GzipCodec</value>
  </property>
  <!-- Speculative execution would violate various assumptions we've made in our system design -->
  <property>
    <name>mapreduce.map.speculative</name>
    <value>false</value>
  </property>
  <property>
    <name>mapreduce.reduce.speculative</name>
    <value>false</value>
  </property>

  <!-- Sometimes jobs take a long time to run, but really, they're okay. Examples: Long index persists,
       hadoop reading lots of empty files into a single mapper. Let's increase the timeout to 30 minutes. -->
  <property>
    <name>mapreduce.task.timeout</name>
    <value>1800000</value>
  </property>  

</configuration>
```

### Yarn-site.xml

```
<configuration>

  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>#{RM_HOSTNAME}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.scheduler.class</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.log-aggregation-enable</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.log.server.url</name>
    <value>http://#{IP_LOG_SERVER}:19888/jobhistory/logs/</value>
  </property>
  <property>
    <name>yarn.nodemanager.hostname</name>
    <value>#{IP_ADDR}</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>512</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>1024</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>1</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
  
  <!-- JBOD -->
  <property>
    <name>yarn.nodemanager.local-dirs</name>
    <value>/mnt/persistent/hadoop/nm-local-dir</value>
  </property>

  <!-- ResourceManager persistent state doesn't work well in tests yet, so disable it -->
  <property>
    <name>yarn.resourcemanager.recovery.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.resourcemanager.store.class</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore</value>
  </property>
  <property>
    <name>yarn.resourcemanager.fs.state-store.uri</name>
    <value>file://${hadoop.tmp.dir}/yarn-resourcemanager-state</value>
  </property>

  <!-- Ability to exclude hosts -->
  <property>
    <name>yarn.resourcemanager.nodes.exclude-path</name>
    <value>/mnt/persistent/hadoop/yarn-exclude.txt</value>
  </property>

</configuration>
```

### HDFS-site.xml

```
<configuration>

  <property>
    <name>dfs.replication</name>
    <value>3</value>
  </property>
  <property>
    <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.hosts.exclude</name>
    <value>/mnt/persistent/hadoop/hdfs-exclude.txt</value>
  </property>

  <!-- JBOD -->
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///mnt/persistent/hadoop/dfs/data</value>
  </property>

</configuration>
```

### Capacity-scheduler.xml

```
<configuration>

  <property>
    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
    <value>0.1</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>default</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.capacity</name>
    <value>100</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.user-limit-factor</name>
    <value>1</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>default</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.maximum-capacity</name>
    <value>100</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.state</name>
    <value>RUNNING</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.acl_submit_applications</name>
    <value>*</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.acl_administer_queue</name>
    <value>*</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.node-locality-delay</name>
    <value>-1</value>
  </property>

</configuration>
```