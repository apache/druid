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

package org.apache.druid.testing.embedded.indexer;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.tools.ITRetryUtil;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class IndexerTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";

  private static final String INDEX_WITH_TIMESTAMP_TASK = "/indexer/wikipedia_with_timestamp_index_task.json";
  // TODO: add queries that validate timestamp is different from the __time column since it is a dimension
  // TODO: https://github.com/apache/druid/issues/9565
  private static final String INDEX_WITH_TIMESTAMP_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";

  private static final String REINDEX_TASK = "/indexer/wikipedia_reindex_task.json";
  private static final String REINDEX_TASK_WITH_DRUID_INPUT_SOURCE = "/indexer/wikipedia_reindex_druid_input_source_task.json";
  private static final String REINDEX_QUERIES_RESOURCE = "/indexer/wikipedia_reindex_queries.json";

  private static final String MERGE_INDEX_TASK = "/indexer/wikipedia_merge_index_task.json";
  private static final String MERGE_INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_merge_index_queries.json";

  private static final String MERGE_REINDEX_TASK = "/indexer/wikipedia_merge_reindex_task.json";
  private static final String MERGE_REINDEX_TASK_WITH_DRUID_INPUT_SOURCE = "/indexer/wikipedia_merge_reindex_druid_input_source_task.json";
  private static final String MERGE_REINDEX_QUERIES_RESOURCE = "/indexer/wikipedia_merge_index_queries.json";

  private static final String INDEX_WITH_MERGE_COLUMN_LIMIT_TASK = "/indexer/wikipedia_index_with_merge_column_limit_task.json";

  private static final CoordinatorDynamicConfig DYNAMIC_CONFIG_PAUSED =
      CoordinatorDynamicConfig.builder().withPauseCoordination(true).build();
  private static final CoordinatorDynamicConfig DYNAMIC_CONFIG_DEFAULT =
      CoordinatorDynamicConfig.builder().build();

  @Override
  protected void addResources(EmbeddedDruidCluster cluster)
  {
    // Testing the legacy config from https://github.com/apache/druid/pull/10267
    // Can remove this when the flag is no longer needed
    cluster.addCommonProperty("druid.indexer.task.ignoreTimestampSpecForDruidInputSource", "true");
  }

  @Test
  public void testIndexData() throws Exception
  {
    final String indexDatasource = dataSource;
    final String reindexDatasource = EmbeddedClusterApis.createTestDatasourceName(getDatasourcePrefix());
    final String reindexDatasourceWithDruidInputSource = EmbeddedClusterApis.createTestDatasourceName(getDatasourcePrefix());
    try (
        final Closeable ignored1 = unloader(indexDatasource);
        final Closeable ignored2 = unloader(reindexDatasource);
        final Closeable ignored3 = unloader(reindexDatasourceWithDruidInputSource)
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
          indexDatasource,
          INDEX_TASK,
          transform,
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
      doReindexTest(
          indexDatasource,
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
    final String indexDatasource = dataSource;
    final String reindexDatasource = EmbeddedClusterApis.createTestDatasourceName(getDatasourcePrefix());
    final String reindexDatasourceWithDruidInputSource = EmbeddedClusterApis.createTestDatasourceName(getDatasourcePrefix());
    try (
        final Closeable ignored1 = unloader(indexDatasource);
        final Closeable ignored2 = unloader(reindexDatasource);
        final Closeable ignored3 = unloader(reindexDatasourceWithDruidInputSource)
    ) {
      doIndexTest(
          indexDatasource,
          INDEX_WITH_TIMESTAMP_TASK,
          INDEX_WITH_TIMESTAMP_QUERIES_RESOURCE,
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
      doReindexTest(
          indexDatasource,
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
    final String fullReindexDatasourceName = dataSource;

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
    final String reindexDatasource = dataSource;
    final String reindexDatasourceWithDruidInputSource = EmbeddedClusterApis.createTestDatasourceName(getDatasourcePrefix());
    try (
        final Closeable ignored2 = unloader(reindexDatasource);
        final Closeable ignored3 = unloader(reindexDatasourceWithDruidInputSource)
    ) {
      doIndexTest(
          reindexDatasource,
          MERGE_INDEX_TASK,
          MERGE_INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          new Pair<>(false, false)
      );
      doReindexTest(
          reindexDatasource,
          reindexDatasource,
          MERGE_REINDEX_TASK,
          MERGE_REINDEX_QUERIES_RESOURCE,
          new Pair<>(false, false)
      );
      doReindexTest(
          reindexDatasource,
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
    final String indexDatasource = dataSource;
    try (final Closeable ignored1 = unloader(indexDatasource)) {
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
          indexDatasource,
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
    final String indexDatasource = dataSource;
    try (
        final Closeable ignored1 = unloader(indexDatasource);
    ) {
      cluster.callApi().onLeaderCoordinator(c -> c.updateCoordinatorDynamicConfig(DYNAMIC_CONFIG_PAUSED));
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
          indexDatasource,
          INDEX_TASK,
          transform,
          INDEX_QUERIES_RESOURCE,
          false,
          false,
          false,
          new Pair<>(true, false)
      );
      cluster.callApi().onLeaderCoordinator(c -> c.updateCoordinatorDynamicConfig(DYNAMIC_CONFIG_DEFAULT));
      cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    }
  }


  @Test
  public void testIndexWithMergeColumnLimitData() throws Exception
  {
    try (final Closeable ignored1 = unloader(dataSource)) {
      doIndexTest(
          dataSource,
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
    final String datasourceName = dataSource;
    try (final Closeable ignored = unloader(datasourceName)) {
      // Submit an Indexing Task
      submitIndexTask(INDEX_TASK, datasourceName);

      // Wait until it acquires a lock
      final List<LockFilterPolicy> lockFilterPolicies = Collections.singletonList(new LockFilterPolicy(datasourceName, 0, null, null));
      final Map<String, List<Interval>> lockedIntervals = new HashMap<>();
      ITRetryUtil.retryUntilFalse(
          () -> {
            lockedIntervals.clear();
            lockedIntervals.putAll(cluster.callApi().onLeaderOverlord(o -> o.findLockedIntervals(lockFilterPolicies)));
            return lockedIntervals.isEmpty();
          },
          "Verify Intervals are Locked"
      );

      // Verify the locked intervals for this datasource
      Assertions.assertEquals(lockedIntervals.size(), 1);
      Assertions.assertEquals(
          List.of(Intervals.of("2013-08-31/2013-09-02")),
          lockedIntervals.get(datasourceName)
      );

      cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    }
  }

  @Test
  public void testJsonFunctions() throws Exception
  {
    final String taskSpec = StringUtils.replace(
        getResourceAsString("/indexer/json_path_index_task.json"),
        PlaceHolders.DATASOURCE,
        dataSource
    );

    submitTaskAndWait(
        taskSpec,
        dataSource,
        false,
        true,
        new Pair<>(false, false)
    );

    cluster.callApi().verifySqlQuery(
        "SELECT __time, \"len\", \"min\", \"max\", \"sum\" FROM %s",
        dataSource,
        "2013-08-31T01:02:33.000Z,5,0,4,10"
    );
  }
}
