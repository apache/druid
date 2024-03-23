/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.tests;

/**
 * These groups are used by continuous integration to split the integration tests into multiple jobs. Any tests that
 * are not annotated with a group will still be run by an "other" integration test continuous integration job.
 */
public class TestNGGroup
{
  public static final String BATCH_INDEX = "batch-index";

  public static final String INPUT_FORMAT = "input-format";

  public static final String INPUT_SOURCE = "input-source";

  public static final String KAFKA_INDEX = "kafka-index";

  public static final String KAFKA_INDEX_SLOW = "kafka-index-slow";

  public static final String TRANSACTIONAL_KAFKA_INDEX = "kafka-transactional-index";

  public static final String TRANSACTIONAL_KAFKA_INDEX_SLOW = "kafka-transactional-index-slow";

  public static final String KAFKA_DATA_FORMAT = "kafka-data-format";

  public static final String COMPACTION = "compaction";

  public static final String UPGRADE = "upgrade";

  public static final String APPEND_INGESTION = "append-ingestion";

  public static final String PERFECT_ROLLUP_PARALLEL_BATCH_INDEX = "perfect-rollup-parallel-batch-index";

  /**
   * This group can only be run individually using -Dgroups=query since it requires specific test data setup.
   */
  public static final String QUERY = "query";

  public static final String QUERY_RETRY = "query-retry";

  public static final String QUERY_ERROR = "query-error";

  public static final String CLI_INDEXER = "cli-indexer";

  public static final String REALTIME_INDEX = "realtime-index";

  /**
   * This group can only be run individually using -Dgroups=security since it requires specific test data setup.
   */
  public static final String SECURITY = "security";

  /**
   * This group can only be run individually using -Dgroups=ldap-security since it requires specific test data setup.
   */
  public static final String LDAP_SECURITY = "ldap-security";

  /**
   * This group is not part of CI. To run this group, s3 configs/credentials for your s3 must be provided in a file.
   * The path of the file must then be pass to mvn with -Doverride.config.path=<PATH_TO_FILE>
   * See integration-tests/docker/environment-configs/override-examples/s3 for env vars to provide.
   */
  public static final String S3_DEEP_STORAGE = "s3-deep-storage";

  /**
   * This group is not part of CI. To run this group, gcs configs/credentials for your gcs must be provided in a file.
   * The path of the file must then be pass to mvn with -Doverride.config.path=<PATH_TO_FILE>
   * See integration-tests/docker/environment-configs/override-examples/gcs for env vars to provide.
   * The path to the folder that contains your GOOGLE_APPLICATION_CREDENTIALS file must also be pass
   * to mvn with -Dresource.file.dir.path=<PATH_TO_FOLDER>
   */
  public static final String GCS_DEEP_STORAGE = "gcs-deep-storage";

  /**
   * This group is not part of CI. To run this group, azure configs/credentials for your azure must be provided in a file.
   * The path of the file must then be pass to mvn with -Doverride.config.path=<PATH_TO_FILE>
   * See integration-tests/docker/environment-configs/override-examples/azures for env vars to provide.
   */
  public static final String AZURE_DEEP_STORAGE = "azure-deep-storage";

  /**
   * This group is not part of CI. To run this group, azure configs/credentials for your oss must be provided in a file.
   * The path of the file must then be pass to mvn with -Doverride.config.path=<PATH_TO_FILE>
   * See integration-tests/docker/environment-configs/override-examples/oss for env vars to provide.
   */
  public static final String ALIYUN_OSS_DEEP_STORAGE = "aliyun-oss-deep-storage";

  /**
   * This group is not part of CI. To run this group, hadoop configs must be provided in a file. The path of the file
   * must then be pass to mvn with -Doverride.config.path=<PATH_TO_FILE>
   * See integration-tests/docker/environment-configs/override-examples/hdfs for env vars to provide.
   * Additionally, hadoop docker must be started by passing -Dstart.hadoop.docker=true to mvn.
   */
  public static final String HDFS_DEEP_STORAGE = "hdfs-deep-storage";

  public static final String HADOOP_S3_TO_S3 = "hadoop-s3-to-s3-deep-storage";
  public static final String HADOOP_S3_TO_HDFS = "hadoop-s3-to-hdfs-deep-storage";

  public static final String HADOOP_AZURE_TO_AZURE = "hadoop-azure-to-azure-deep-storage";
  public static final String HADOOP_AZURE_TO_HDFS = "hadoop-azure-to-hdfs-deep-storage";

  public static final String HADOOP_GCS_TO_GCS = "hadoop-gcs-to-gcs-deep-storage";
  public static final String HADOOP_GCS_TO_HDFS = "hadoop-gcs-to-hdfs-deep-storage";

  /**
   * This group is not part of CI. To run this group, s3 configs/credentials for your s3 must be provided in a file.
   * The path of the file must then be pass to mvn with -Doverride.config.path=<PATH_TO_FILE>
   * See integration-tests/docker/environment-configs/override-examples/s3 for env vars to provide.
   */
  public static final String S3_INGESTION = "s3-ingestion";

  /**
   * This group is not part of CI explicitly. It allows you to run all the tests that have been tested with
   * against a quickstart deployment of Druid using the instructions in this project's README.
   * No tests should belong exclusively to this group. It is only meant to be a helper group to run tests against
   * a local quickstart deployment.
   *
   * As you run tests in your environment, mark the tests as quickstart compatible (see
   * {@link org.apache.druid.tests.indexer.ITIndexerTest}) and add any additional instructions that were needed to
   * get the tests to work to this project's README.
   */
  public static final String QUICKSTART_COMPATIBLE = "quickstart-compatible";

  /**
   * This group is not part of CI. To run this group, AWS kinesis configs/credentials for your AWS kinesis must be
   * provided in a file. The path of the file must then be pass to mvn with -Doverride.config.path=<PATH_TO_FILE>
   * See integration-tests/docker/environment-configs/override-examples/kinesis for env vars to provide.
   * Kinesis stream endpoint for a region must also be pass to mvn with -Ddruid.test.config.streamEndpoint=<ENDPOINT>
   */
  public static final String KINESIS_INDEX = "kinesis-index";

  /**
   * This group is not part of CI. To run this group, AWS kinesis configs/credentials for your AWS kinesis must be
   * provided in a file. The path of the file must then be pass to mvn with -Doverride.config.path=<PATH_TO_FILE>
   * See integration-tests/docker/environment-configs/override-examples/kinesis for env vars to provide.
   * Kinesis stream endpoint for a region must also be pass to mvn with -Ddruid.test.config.streamEndpoint=<ENDPOINT>
   */
  public static final String KINESIS_DATA_FORMAT = "kinesis-data-format";

  public static final String HIGH_AVAILABILTY = "high-availability";

  public static final String SHUFFLE_DEEP_STORE = "shuffle-deep-store";

  public static final String CUSTOM_COORDINATOR_DUTIES = "custom-coordinator-duties";

  public static final String HTTP_ENDPOINT = "http-endpoint";

  public static final String CENTRALIZED_DATASOURCE_SCHEMA = "centralized-datasource-schema";
}
