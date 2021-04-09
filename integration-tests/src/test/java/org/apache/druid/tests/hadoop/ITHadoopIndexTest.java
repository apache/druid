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

package org.apache.druid.tests.hadoop;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.indexer.partitions.DimensionBasedPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractITBatchIndexTest;
import org.apache.druid.timeline.partition.HashPartitionFunction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.UUID;
import java.util.function.Function;

/**
 * IMPORTANT:
 * To run this test, you must:
 * 1) Copy wikipedia_index_data1.json, wikipedia_index_data2.json, and wikipedia_index_data3.json
 *    located in integration-tests/src/test/resources/data/batch_index/json to your HDFS at /batch_index/json/
 *    If using the Docker-based Hadoop container, this is automatically done by the integration tests.
 * 2) Copy batch_hadoop.data located in integration-tests/src/test/resources/data/batch_index/hadoop_tsv to your HDFS
 *    at /batch_index/hadoop_tsv/
 *    If using the Docker-based Hadoop container, this is automatically done by the integration tests.
 * 2) Provide -Doverride.config.path=<PATH_TO_FILE> with HDFS configs set. See
 *    integration-tests/docker/environment-configs/override-examples/hdfs for env vars to provide.
 * 3) Run the test with -Dstart.hadoop.docker=true -Dextra.datasource.name.suffix='' in the mvn command
 */
@Test(groups = TestNGGroup.HDFS_DEEP_STORAGE)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITHadoopIndexTest extends AbstractITBatchIndexTest
{
  private static final Logger LOG = new Logger(ITHadoopIndexTest.class);

  private static final String BATCH_TASK = "/hadoop/batch_hadoop_indexer.json";
  private static final String BATCH_QUERIES_RESOURCE = "/hadoop/batch_hadoop_queries.json";
  private static final String BATCH_DATASOURCE = "batchLegacyHadoop";

  private static final String INDEX_TASK = "/hadoop/wikipedia_hadoop_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INDEX_DATASOURCE = "wikipedia_hadoop_index_test";

  private static final String REINDEX_TASK = "/hadoop/wikipedia_hadoop_reindex_task.json";
  private static final String REINDEX_QUERIES_RESOURCE = "/indexer/wikipedia_reindex_queries.json";
  private static final String REINDEX_DATASOURCE = "wikipedia_hadoop_reindex_test";

  private static final CoordinatorDynamicConfig DYNAMIC_CONFIG_PAUSED =
      CoordinatorDynamicConfig.builder().withPauseCoordination(true).build();
  private static final CoordinatorDynamicConfig DYNAMIC_CONFIG_DEFAULT =
      CoordinatorDynamicConfig.builder().build();

  @Inject
  CoordinatorResourceTestClient coordinatorClient;

  @DataProvider
  public static Object[][] resources()
  {
    return new Object[][]{
        {new HashedPartitionsSpec(3, null, null)},
        {new HashedPartitionsSpec(null, 3, ImmutableList.of("page"))},
        {new HashedPartitionsSpec(null, 3, ImmutableList.of("page", "user"))},
        {new HashedPartitionsSpec(null, 3, ImmutableList.of("page"), HashPartitionFunction.MURMUR3_32_ABS)},
        {new SingleDimensionPartitionsSpec(1000, null, null, false)},
        {new SingleDimensionPartitionsSpec(1000, null, "page", false)},
        {new SingleDimensionPartitionsSpec(1000, null, null, true)},

        //{new HashedPartitionsSpec(null, 3, null)} // this results in a bug where the segments have 0 rows
    };
  }

  @Test
  public void testLegacyITHadoopIndexTest() throws Exception
  {
    String indexDatasource = BATCH_DATASOURCE + "_" + UUID.randomUUID();
    try (
        final Closeable ignored0 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> specPathsTransform = spec -> {
        try {
          String path = "/batch_index/tsv";
          spec = StringUtils.replace(
              spec,
              "%%INPUT_PATHS%%",
              path
          );

          return spec;
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          indexDatasource,
          BATCH_TASK,
          specPathsTransform,
          BATCH_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(false, false)
      );
    }
  }

  @Test(dataProvider = "resources")
  public void testIndexData(DimensionBasedPartitionsSpec partitionsSpec) throws Exception
  {
    String indexDatasource = INDEX_DATASOURCE + "_" + UUID.randomUUID();
    String reindexDatasource = REINDEX_DATASOURCE + "_" + UUID.randomUUID();

    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
        final Closeable ignored2 = unloader(reindexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> specPathsTransform = spec -> {
        try {
          String path = "/batch_index/json";
          spec = StringUtils.replace(
              spec,
              "%%INPUT_PATHS%%",
              path
          );
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(partitionsSpec)
          );
          spec = StringUtils.replace(
              spec,
              "%%SEGMENT_AVAIL_TIMEOUT_MILLIS%%",
              jsonMapper.writeValueAsString(0)
          );

          return spec;
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          indexDatasource,
          INDEX_TASK,
          specPathsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(false, false)
      );

      doReindexTest(
          indexDatasource,
          reindexDatasource,
          REINDEX_TASK,
          REINDEX_QUERIES_RESOURCE,
          new Pair<>(false, false)
      );
    }
  }

  /**
   * Test Hadoop Batch Ingestion with a non-zero value for awaitSegmentAvailabilityTimeoutMillis. This will confirm that
   * the report for the task indicates segments were confirmed to be available on the cluster before finishing the job.
   *
   * @throws Exception
   */
  @Test
  public void testIndexDataAwaitSegmentAvailability() throws Exception
  {
    String indexDatasource = INDEX_DATASOURCE + "_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> specPathsTransform = spec -> {
        try {
          String path = "/batch_index/json";
          spec = StringUtils.replace(
              spec,
              "%%INPUT_PATHS%%",
              path
          );
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(
                  new HashedPartitionsSpec(3, null, null)
              )
          );
          spec = StringUtils.replace(
              spec,
              "%%SEGMENT_AVAIL_TIMEOUT_MILLIS%%",
              jsonMapper.writeValueAsString(600000)
          );

          return spec;
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          indexDatasource,
          INDEX_TASK,
          specPathsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(true, true)
      );
    }
  }

  /**
   * Test Hadoop Batch Indexing with non-zero value for awaitSegmentAvailabilityTimeoutMillis. The coordinator
   * is paused when the task runs. This should result in a successful task with a flag in the task report indicating
   * that we did not confirm segment availability.
   *
   * @throws Exception
   */
  @Test
  public void testIndexDataAwaitSegmentAvailabilityFailsButTaskSucceeds() throws Exception
  {
    String indexDatasource = INDEX_DATASOURCE + "_" + UUID.randomUUID();

    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      coordinatorClient.postDynamicConfig(DYNAMIC_CONFIG_PAUSED);
      final Function<String, String> specPathsTransform = spec -> {
        try {
          String path = "/batch_index/json";
          spec = StringUtils.replace(
              spec,
              "%%INPUT_PATHS%%",
              path
          );
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(
                  new HashedPartitionsSpec(3, null, null)
              )
          );
          spec = StringUtils.replace(
              spec,
              "%%SEGMENT_AVAIL_TIMEOUT_MILLIS%%",
              jsonMapper.writeValueAsString(1)
          );

          return spec;
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          indexDatasource,
          INDEX_TASK,
          specPathsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          false,
          false,
          new Pair<>(true, false)
      );
      coordinatorClient.postDynamicConfig(DYNAMIC_CONFIG_DEFAULT);
      ITRetryUtil.retryUntilTrue(
          () -> coordinatorClient.areSegmentsLoaded(indexDatasource + config.getExtraDatasourceNameSuffix()), "Segment Load For: " + indexDatasource + config.getExtraDatasourceNameSuffix()
      );
    }
  }
}
