---
id: hdfs
title: "HDFS"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


To use this Apache Druid extension, make sure to [include](../../development/extensions.md#loading-extensions) `druid-hdfs-storage` as an extension and run druid processes with `GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_keyfile` in the environment.

## Deep Storage

### Configuration for HDFS

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs||Must be set.|
|`druid.storage.storageDirectory`||Directory for storing segments.|Must be set.|
|`druid.hadoop.security.kerberos.principal`|`druid@EXAMPLE.COM`| Principal user name |empty|
|`druid.hadoop.security.kerberos.keytab`|`/etc/security/keytabs/druid.headlessUser.keytab`|Path to keytab file|empty|

Besides the above settings, you also need to include all Hadoop configuration files (such as `core-site.xml`, `hdfs-site.xml`)
in the Druid classpath. One way to do this is copying all those files under `${DRUID_HOME}/conf/_common`.

If you are using the Hadoop ingestion, set your output directory to be a location on Hadoop and it will work.
If you want to eagerly authenticate against a secured hadoop/hdfs cluster you must set `druid.hadoop.security.kerberos.principal` and `druid.hadoop.security.kerberos.keytab`, this is an alternative to the cron job method that runs `kinit` command periodically.

### Configuration for Cloud Storage

You can also use the AWS S3 or the Google Cloud Storage as the deep storage via HDFS.

#### Configuration for AWS S3

To use the AWS S3 as the deep storage, you need to configure `druid.storage.storageDirectory` properly.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs| |Must be set.|
|`druid.storage.storageDirectory`|s3a://bucket/example/directory or s3n://bucket/example/directory|Path to the deep storage|Must be set.|

You also need to include the [Hadoop AWS module](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html), especially the `hadoop-aws.jar` in the Druid classpath.
Run the below command to install the `hadoop-aws.jar` file under `${DRUID_HOME}/extensions/druid-hdfs-storage` in all nodes.

```bash
java -classpath "${DRUID_HOME}lib/*" org.apache.druid.cli.Main tools pull-deps -h "org.apache.hadoop:hadoop-aws:${HADOOP_VERSION}";
cp ${DRUID_HOME}/hadoop-dependencies/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar ${DRUID_HOME}/extensions/druid-hdfs-storage/
```

Finally, you need to add the below properties in the `core-site.xml`.
For more configurations, see the [Hadoop AWS module](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html).

```xml
<property>
  <name>fs.s3a.impl</name>
  <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  <description>The implementation class of the S3A Filesystem</description>
</property>

<property>
  <name>fs.AbstractFileSystem.s3a.impl</name>
  <value>org.apache.hadoop.fs.s3a.S3A</value>
  <description>The implementation class of the S3A AbstractFileSystem.</description>
</property>

<property>
  <name>fs.s3a.access.key</name>
  <description>AWS access key ID. Omit for IAM role-based or provider-based authentication.</description>
  <value>your access key</value>
</property>

<property>
  <name>fs.s3a.secret.key</name>
  <description>AWS secret key. Omit for IAM role-based or provider-based authentication.</description>
  <value>your secret key</value>
</property>
```

#### Configuration for Google Cloud Storage

To use the Google Cloud Storage as the deep storage, you need to configure `druid.storage.storageDirectory` properly.

|Property|Possible Values|Description|Default|
|--------|---------------|-----------|-------|
|`druid.storage.type`|hdfs||Must be set.|
|`druid.storage.storageDirectory`|gs://bucket/example/directory|Path to the deep storage|Must be set.|

All services that need to access GCS need to have the [GCS connector jar](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage#other_sparkhadoop_clusters) in their class path.
Please read the [install instructions](https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/INSTALL.md)
to properly set up the necessary libraries and configurations.
One option is to place this jar in `${DRUID_HOME}/lib/` and `${DRUID_HOME}/extensions/druid-hdfs-storage/`.

Finally, you need to configure the `core-site.xml` file with the filesystem
and authentication properties needed for GCS. You may want to copy the below
example properties. Please follow the instructions at
[https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/INSTALL.md](https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/INSTALL.md)
for more details.
For more configurations, [GCS core default](https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/conf/gcs-core-default.xml)
and [GCS core template](https://github.com/GoogleCloudPlatform/bdutil/blob/master/conf/hadoop2/gcs-core-template.xml).

```xml
<property>
  <name>fs.gs.impl</name>
  <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
  <description>The FileSystem for gs: (GCS) uris.</description>
</property>

<property>
  <name>fs.AbstractFileSystem.gs.impl</name>
  <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
  <description>The AbstractFileSystem for gs: uris.</description>
</property>

<property>
  <name>google.cloud.auth.service.account.enable</name>
  <value>true</value>
  <description>
    Whether to use a service account for GCS authorization.
    Setting this property to `false` will disable use of service accounts for
    authentication.
  </description>
</property>

<property>
  <name>google.cloud.auth.service.account.json.keyfile</name>
  <value>/path/to/keyfile</value>
  <description>
    The JSON key file of the service account used for GCS
    access when google.cloud.auth.service.account.enable is true.
  </description>
</property>
```

Tested with Druid 0.17.0, Hadoop 2.8.5 and gcs-connector jar 2.0.0-hadoop2.

## Reading data from HDFS or Cloud Storage

### Native batch ingestion

The [HDFS input source](../../ingestion/native-batch.md#hdfs-input-source) is supported by the [Parallel task](../../ingestion/native-batch.md#parallel-task)
to read files directly from the HDFS Storage. You may be able to read objects from cloud storage
with the HDFS input source, but we highly recommend to use a proper
[Input Source](../../ingestion/native-batch.md#input-sources) instead if possible because
it is simple to set up. For now, only the [S3 input source](../../ingestion/native-batch.md#s3-input-source)
and the [Google Cloud Storage input source](../../ingestion/native-batch.md#google-cloud-storage-input-source)
are supported for cloud storage types, and so you may still want to use the HDFS input source
to read from cloud storage other than those two.

### Hadoop-based ingestion

If you use the [Hadoop ingestion](../../ingestion/hadoop.md), you can read data from HDFS
by specifying the paths in your [`inputSpec`](../../ingestion/hadoop.md#inputspec).
See the [Static](../../ingestion/hadoop.md#static) inputSpec for details.
