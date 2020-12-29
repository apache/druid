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

package org.apache.druid.tests.indexer;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.function.Function;

@Test(groups = {TestNGGroup.BATCH_INDEX, TestNGGroup.QUICKSTART_COMPATIBLE})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITIndexerTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static final String INDEX_TASK_NULL_INTERVALS = "/indexer/wikipedia_index_task_null_intervals.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INDEX_DATASOURCE = "wikipedia_index_test";

  private static final String INDEX_WITH_TIMESTAMP_TASK = "/indexer/wikipedia_with_timestamp_index_task.json";
  // TODO: add queries that validate timestamp is different from the __time column since it is a dimension
  // TODO: https://github.com/apache/druid/issues/9565
  private static final String INDEX_WITH_TIMESTAMP_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INDEX_WITH_TIMESTAMP_DATASOURCE = "wikipedia_with_timestamp_index_test";

  private static final String REINDEX_TASK = "/indexer/wikipedia_reindex_task.json";
  private static final String REINDEX_TASK_WITH_DRUID_INPUT_SOURCE = "/indexer/wikipedia_reindex_druid_input_source_task.json";
  private static final String REINDEX_QUERIES_RESOURCE = "/indexer/wikipedia_reindex_queries.json";
  private static final String REINDEX_DATASOURCE = "wikipedia_reindex_test";

  private static final String MERGE_INDEX_TASK = "/indexer/wikipedia_merge_index_task.json";
  private static final String MERGE_INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_merge_index_queries.json";
  private static final String MERGE_INDEX_DATASOURCE = "wikipedia_merge_index_test";

  private static final String MERGE_REINDEX_TASK = "/indexer/wikipedia_merge_reindex_task.json";
  private static final String MERGE_REINDEX_TASK_WITH_DRUID_INPUT_SOURCE = "/indexer/wikipedia_merge_reindex_druid_input_source_task.json";
  private static final String MERGE_REINDEX_QUERIES_RESOURCE = "/indexer/wikipedia_merge_index_queries.json";
  private static final String MERGE_REINDEX_DATASOURCE = "wikipedia_merge_reindex_test";

  @DataProvider
  public static Object[][] failureResourcesZeroMaxThenMaxZero()
  {
    return new Object[][]{
        {0, Integer.MAX_VALUE},
        {Integer.MAX_VALUE, 0}
    };
  }

  @DataProvider
  public static Object[][] failureResourcesMaxZero()
  {
    return new Object[][]{
        {Integer.MAX_VALUE, 0}
    };
  }

  @Test
  public void testIndexData() throws Exception
  {
    final String reindexDatasource = REINDEX_DATASOURCE + "-testIndexData";
    final String reindexDatasourceWithDruidInputSource = REINDEX_DATASOURCE + "-testIndexData-druidInputSource";
    try (
        final Closeable ignored1 = unloader(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix());
        final Closeable ignored2 = unloader(reindexDatasource + config.getExtraDatasourceNameSuffix());
        final Closeable ignored3 = unloader(reindexDatasourceWithDruidInputSource + config.getExtraDatasourceNameSuffix())
    ) {
      final Function<String, String> transform = spec -> {
        try {
          spec = StringUtils.replace(
              spec,
              "%%MAX_SEGMENT_INTERVALS_PERMITTED%%",
              jsonMapper.writeValueAsString(Integer.MAX_VALUE)
          );
          spec = StringUtils.replace(
              spec,
              "%%MAX_SEGMENT_INTERVAL_SHARDS_PERMITTED%%",
              jsonMapper.writeValueAsString(Integer.MAX_VALUE)
          );
          spec = StringUtils.replace(
              spec,
              "%%FORCE_GUARANTEED_ROLLUP%%",
              jsonMapper.writeValueAsString(false)
          );

          return spec;
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          INDEX_DATASOURCE,
          INDEX_TASK,
          transform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          true
      );
      doReindexTest(
          INDEX_DATASOURCE,
          reindexDatasource,
          REINDEX_TASK,
          REINDEX_QUERIES_RESOURCE
      );
      doReindexTest(
          INDEX_DATASOURCE,
          reindexDatasourceWithDruidInputSource,
          REINDEX_TASK_WITH_DRUID_INPUT_SOURCE,
          REINDEX_QUERIES_RESOURCE
      );
    }
  }

  @Test
  public void testReIndexDataWithTimestamp() throws Exception
  {
    final String reindexDatasource = REINDEX_DATASOURCE + "-testReIndexDataWithTimestamp";
    final String reindexDatasourceWithDruidInputSource = REINDEX_DATASOURCE + "-testReIndexDataWithTimestamp-druidInputSource";
    try (
        final Closeable ignored1 = unloader(INDEX_WITH_TIMESTAMP_DATASOURCE + config.getExtraDatasourceNameSuffix());
        final Closeable ignored2 = unloader(reindexDatasource + config.getExtraDatasourceNameSuffix());
        final Closeable ignored3 = unloader(reindexDatasourceWithDruidInputSource + config.getExtraDatasourceNameSuffix())
    ) {
      doIndexTest(
          INDEX_WITH_TIMESTAMP_DATASOURCE,
          INDEX_WITH_TIMESTAMP_TASK,
          INDEX_WITH_TIMESTAMP_QUERIES_RESOURCE,
          false,
          true,
          true,
          true
      );
      doReindexTest(
          INDEX_WITH_TIMESTAMP_DATASOURCE,
          reindexDatasource,
          REINDEX_TASK,
          REINDEX_QUERIES_RESOURCE
      );
      doReindexTest(
          INDEX_WITH_TIMESTAMP_DATASOURCE,
          reindexDatasourceWithDruidInputSource,
          REINDEX_TASK_WITH_DRUID_INPUT_SOURCE,
          REINDEX_QUERIES_RESOURCE
      );
    }
  }

  @Test
  public void testMERGEIndexData() throws Exception
  {
    final String reindexDatasource = MERGE_REINDEX_DATASOURCE + "-testMergeIndexData";
    final String reindexDatasourceWithDruidInputSource = MERGE_REINDEX_DATASOURCE + "-testMergeReIndexData-druidInputSource";
    try (
        final Closeable ignored1 = unloader(MERGE_INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix());
        final Closeable ignored2 = unloader(reindexDatasource + config.getExtraDatasourceNameSuffix());
        final Closeable ignored3 = unloader(reindexDatasourceWithDruidInputSource + config.getExtraDatasourceNameSuffix())
    ) {
      doIndexTest(
          MERGE_INDEX_DATASOURCE,
          MERGE_INDEX_TASK,
          MERGE_INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          true
      );
      doReindexTest(
          MERGE_INDEX_DATASOURCE,
          reindexDatasource,
          MERGE_REINDEX_TASK,
          MERGE_REINDEX_QUERIES_RESOURCE
      );
      doReindexTest(
          MERGE_INDEX_DATASOURCE,
          reindexDatasourceWithDruidInputSource,
          MERGE_REINDEX_TASK_WITH_DRUID_INPUT_SOURCE,
          MERGE_INDEX_QUERIES_RESOURCE
      );
    }
  }

  /**
   * Test that ingestion properly fails due to tuningConfig paramaters. Both maxSegmentIntervalsPermitted and
   * maxAggregateSegmentIntervalShardsPermitted are tested for ingestion with null intervals and hashed partitioning.
   *
   * @param resourceArray tuningConfig values to populate ingestion spec.
   * @throws Exception
   */
  @Test(dataProvider = "failureResourcesZeroMaxThenMaxZero")
  public void testHashedPartitioningNullIntervalsIndexFailure(int[] resourceArray) throws Exception
  {
    final Function<String, String> transform = spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%MAX_SEGMENT_INTERVALS_PERMITTED%%",
            jsonMapper.writeValueAsString(resourceArray[0])
        );
        spec = StringUtils.replace(
            spec,
            "%%MAX_SEGMENT_INTERVAL_SHARDS_PERMITTED%%",
            jsonMapper.writeValueAsString(resourceArray[1])
        );
        spec = StringUtils.replace(
            spec,
            "%%FORCE_GUARANTEED_ROLLUP%%",
            jsonMapper.writeValueAsString(true)
        );

        return spec;
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    doIndexTest(
        INDEX_DATASOURCE,
        INDEX_TASK_NULL_INTERVALS,
        transform,
        INDEX_QUERIES_RESOURCE,
        false,
        false,
        false,
        false
    );
  }

  /**
   * Test that ingestion properly fails due to tuningConfig paramaters. Both maxSegmentIntervalsPermitted and
   * maxAggregateSegmentIntervalShardsPermitted are tested for ingestion with non-null intervals and hashed partitioning.
   *
   * @param resourceArray tuningConfig values to populate ingestion spec.
   * @throws Exception
   */
  @Test(dataProvider = "failureResourcesZeroMaxThenMaxZero")
  public void testHashedPartitioningNonNullIntervalsIndexFailure(int[] resourceArray) throws Exception
  {
    final Function<String, String> transform = spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%MAX_SEGMENT_INTERVALS_PERMITTED%%",
            jsonMapper.writeValueAsString(resourceArray[0])
        );
        spec = StringUtils.replace(
            spec,
            "%%MAX_SEGMENT_INTERVAL_SHARDS_PERMITTED%%",
            jsonMapper.writeValueAsString(resourceArray[1])
        );
        spec = StringUtils.replace(
            spec,
            "%%FORCE_GUARANTEED_ROLLUP%%",
            jsonMapper.writeValueAsString(true)
        );

        return spec;
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    doIndexTest(
        INDEX_DATASOURCE,
        INDEX_TASK,
        transform,
        INDEX_QUERIES_RESOURCE,
        false,
        false,
        false,
        false
    );
  }

  /**
   * Test that ingestion properly fails due to tuningConfig paramaters. maxSegmentIntervalsPermitted is tested for
   * ingestion with null intervals and dynamic partitioning.
   *
   * @param resourceArray tuningConfig values to populate ingestion spec.
   * @throws Exception
   */
  @Test(dataProvider = "failureResourcesMaxZero")
  public void testDynamicPartitioningNullIntervalsIndexFailure(int[] resourceArray) throws Exception
  {
    final Function<String, String> transform = spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%MAX_SEGMENT_INTERVALS_PERMITTED%%",
            jsonMapper.writeValueAsString(resourceArray[0])
        );
        spec = StringUtils.replace(
            spec,
            "%%MAX_SEGMENT_INTERVAL_SHARDS_PERMITTED%%",
            jsonMapper.writeValueAsString(resourceArray[1])
        );
        spec = StringUtils.replace(
            spec,
            "%%FORCE_GUARANTEED_ROLLUP%%",
            jsonMapper.writeValueAsString(false)
        );

        return spec;
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    doIndexTest(
        INDEX_DATASOURCE,
        INDEX_TASK_NULL_INTERVALS,
        transform,
        INDEX_QUERIES_RESOURCE,
        false,
        false,
        false,
        false
    );
  }
}
