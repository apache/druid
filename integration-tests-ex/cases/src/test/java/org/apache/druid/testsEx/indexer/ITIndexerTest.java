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

package org.apache.druid.testsEx.indexer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testsEx.categories.BatchIndex;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@RunWith(DruidTestRunner.class)
@Category(BatchIndex.class)
public class ITIndexerTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
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

  private static final String INDEX_WITH_MERGE_COLUMN_LIMIT_TASK = "/indexer/wikipedia_index_with_merge_column_limit_task.json";
  private static final String INDEX_WITH_MERGE_COLUMN_LIMIT_DATASOURCE = "wikipedia_index_with_merge_column_limit_test";

  private static final String GET_LOCKED_INTERVALS = "wikipedia_index_get_locked_intervals_test";

  private static final CoordinatorDynamicConfig DYNAMIC_CONFIG_PAUSED =
      CoordinatorDynamicConfig.builder().withPauseCoordination(true).build();
  private static final CoordinatorDynamicConfig DYNAMIC_CONFIG_DEFAULT =
      CoordinatorDynamicConfig.builder().build();

  @Inject
  CoordinatorResourceTestClient coordinatorClient;

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
          return StringUtils.replace(
              spec,
              "%%SEGMENT_AVAIL_TIMEOUT_MILLIS%%",
              jsonMapper.writeValueAsString("0")
          );
        }
        catch (JsonProcessingException e) {
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
          new Pair<>(false, false)
      );
      doReindexTest(
          INDEX_DATASOURCE,
          reindexDatasource,
          REINDEX_TASK,
          REINDEX_QUERIES_RESOURCE,
          new Pair<>(false, false)
      );
      doReindexTest(
          INDEX_DATASOURCE,
          reindexDatasourceWithDruidInputSource,
          REINDEX_TASK_WITH_DRUID_INPUT_SOURCE,
          REINDEX_QUERIES_RESOURCE,
          new Pair<>(false, false)
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
          new Pair<>(false, false)
      );
      doReindexTest(
          INDEX_WITH_TIMESTAMP_DATASOURCE,
          reindexDatasource,
          REINDEX_TASK,
          REINDEX_QUERIES_RESOURCE,
          new Pair<>(false, false)
      );
      doReindexTest(
          INDEX_WITH_TIMESTAMP_DATASOURCE,
          reindexDatasourceWithDruidInputSource,
          REINDEX_TASK_WITH_DRUID_INPUT_SOURCE,
          REINDEX_QUERIES_RESOURCE,
          new Pair<>(false, false)
      );
    }
  }

  @Test
  public void testReIndexWithNonExistingDatasource() throws Exception
  {
    Pair<Boolean, Boolean> dummyPair = new Pair<>(false, false);
    final String fullBaseDatasourceName = "nonExistingDatasource2904";
    final String fullReindexDatasourceName = "newDatasource123";

    String taskSpec = StringUtils.replace(
        getResourceAsString(REINDEX_TASK_WITH_DRUID_INPUT_SOURCE),
        "%%DATASOURCE%%",
        fullBaseDatasourceName
    );
    taskSpec = StringUtils.replace(
        taskSpec,
        "%%REINDEX_DATASOURCE%%",
        fullReindexDatasourceName
    );

    // This method will also verify task is successful after task finish running
    // We expect task to be successful even if the datasource to reindex does not exist
    submitTaskAndWait(
        taskSpec,
        fullReindexDatasourceName,
        false,
        false,
        dummyPair
    );
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
          new Pair<>(false, false)
      );
      doReindexTest(
          MERGE_INDEX_DATASOURCE,
          reindexDatasource,
          MERGE_REINDEX_TASK,
          MERGE_REINDEX_QUERIES_RESOURCE,
          new Pair<>(false, false)
      );
      doReindexTest(
          MERGE_INDEX_DATASOURCE,
          reindexDatasourceWithDruidInputSource,
          MERGE_REINDEX_TASK_WITH_DRUID_INPUT_SOURCE,
          MERGE_INDEX_QUERIES_RESOURCE,
          new Pair<>(false, false)
      );
    }
  }

  /**
   * Test that task reports indicate the ingested segments were loaded before the configured timeout expired.
   *
   * @throws Exception
   */
  @Test
  public void testIndexDataAwaitSegmentAvailability() throws Exception
  {
    try (
        final Closeable ignored1 = unloader(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> transform = spec -> {
        try {
          return StringUtils.replace(
              spec,
              "%%SEGMENT_AVAIL_TIMEOUT_MILLIS%%",
              jsonMapper.writeValueAsString("600000")
          );
        }
        catch (JsonProcessingException e) {
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
          new Pair<>(true, true)
      );
    }
  }

  /**
   * Test that the task still succeeds if the segments do not become available before the configured wait timeout
   * expires.
   *
   * @throws Exception
   */
  @Test
  public void testIndexDataAwaitSegmentAvailabilityFailsButTaskSucceeds() throws Exception
  {
    try (
        final Closeable ignored1 = unloader(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix());
    ) {
      coordinatorClient.postDynamicConfig(DYNAMIC_CONFIG_PAUSED);
      final Function<String, String> transform = spec -> {
        try {
          return StringUtils.replace(
              spec,
              "%%SEGMENT_AVAIL_TIMEOUT_MILLIS%%",
              jsonMapper.writeValueAsString("1")
          );
        }
        catch (JsonProcessingException e) {
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
          new Pair<>(true, false)
      );
      coordinatorClient.postDynamicConfig(DYNAMIC_CONFIG_DEFAULT);
      ITRetryUtil.retryUntilTrue(
          () -> coordinator.areSegmentsLoaded(INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix()), "Segment Load"
      );
    }
  }


  @Test
  public void testIndexWithMergeColumnLimitData() throws Exception
  {
    try (
        final Closeable ignored1 = unloader(INDEX_WITH_MERGE_COLUMN_LIMIT_DATASOURCE + config.getExtraDatasourceNameSuffix());
    ) {
      doIndexTest(
          INDEX_WITH_MERGE_COLUMN_LIMIT_DATASOURCE,
          INDEX_WITH_MERGE_COLUMN_LIMIT_TASK,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(false, false)
      );
    }
  }

  @Test
  public void testGetLockedIntervals() throws Exception
  {
    final String datasourceName = GET_LOCKED_INTERVALS + config.getExtraDatasourceNameSuffix();
    try (final Closeable ignored = unloader(datasourceName)) {
      // Submit an Indexing Task
      submitIndexTask(INDEX_TASK, datasourceName);

      // Wait until it acquires a lock
      final Map<String, Integer> minTaskPriority = Collections.singletonMap(datasourceName, 0);
      final Map<String, List<Interval>> lockedIntervals = new HashMap<>();
      ITRetryUtil.retryUntilFalse(
          () -> {
            lockedIntervals.clear();
            lockedIntervals.putAll(indexer.getLockedIntervals(minTaskPriority));
            return lockedIntervals.isEmpty();
          },
          "Verify Intervals are Locked"
      );

      // Verify the locked intervals for this datasource
      Assert.assertEquals(lockedIntervals.size(), 1);
      Assert.assertEquals(
          lockedIntervals.get(datasourceName),
          Collections.singletonList(Intervals.of("2013-08-31/2013-09-02"))
      );

      ITRetryUtil.retryUntilTrue(
          () -> coordinator.areSegmentsLoaded(datasourceName),
          "Segment Load"
      );
    }
  }

  @Test
  public void testJsonFunctions() throws Exception
  {
    final String taskSpec = getResourceAsString("/indexer/json_path_index_task.json");

    submitTaskAndWait(
        taskSpec,
        "json_path_index_test",
        false,
        true,
        new Pair<>(false, false)
    );

    doTestQuery("json_path_index_test", "/indexer/json_path_index_queries.json");
  }
}
