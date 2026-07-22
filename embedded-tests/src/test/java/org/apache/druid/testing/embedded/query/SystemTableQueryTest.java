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

package org.apache.druid.testing.embedded.query;


import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration test for system table queries.
 * In this test we're using deterministic table names to avoid flaky behavior of the test.
 */
public class SystemTableQueryTest extends QueryTestBase
{
  /**
   * The per-datasource aggregates the web console's {@code GROUP BY datasource} query issues over
   * sys.segments. Kept identical between the accelerated (no WHERE -> SegmentsRollupRule) and the
   * scan (WHERE datasource=... -> rule does not match) forms so the two can be compared.
   */
  private static final String DASHBOARD_AGGREGATES =
      "COUNT(*) FILTER (WHERE is_active = 1) AS num_segments, "
      + "COUNT(*) FILTER (WHERE is_published = 1 AND is_overshadowed = 0 AND replication_factor = 0) AS num_zero_replica_segments, "
      + "COUNT(*) FILTER (WHERE is_published = 1 AND is_overshadowed = 0 AND is_available = 0 AND replication_factor > 0) AS num_segments_to_load, "
      + "COUNT(*) FILTER (WHERE is_available = 1 AND is_active = 0) AS num_segments_to_drop, "
      + "SUM(\"size\") FILTER (WHERE is_active = 1) AS total_data_size, "
      + "MIN(\"num_rows\") FILTER (WHERE is_available = 1 AND is_realtime = 0) AS min_segment_rows, "
      + "AVG(\"num_rows\") FILTER (WHERE is_available = 1 AND is_realtime = 0) AS avg_segment_rows, "
      + "MAX(\"num_rows\") FILTER (WHERE is_available = 1 AND is_realtime = 0) AS max_segment_rows, "
      + "SUM(\"num_rows\") FILTER (WHERE is_active = 1) AS total_rows, "
      + "CASE WHEN SUM(\"num_rows\") FILTER (WHERE is_available = 1) <> 0 "
      + "THEN (SUM(\"size\") FILTER (WHERE is_available = 1) / SUM(\"num_rows\") FILTER (WHERE is_available = 1)) "
      + "ELSE 0 END AS avg_row_size, "
      + "SUM(\"size\" * \"num_replicas\") FILTER (WHERE is_active = 1) AS replicated_size";

  private static final String ROLLUP_SYNC_METRIC = "segment/rollup/sync/time";
  private static final String ROLLUP_QUERY_METRIC = "segment/rollup/query/count";

  private String testDataSourceName;

  @Override
  public void beforeAll()
  {
    testDataSourceName = ingestBasicData();
  }

  @Test
  public void testSystemTableQueries_segmentsCount()
  {
    String query = StringUtils.format(
        "SELECT datasource, count(*) \n"
        + "FROM sys.segments \n"
        + "WHERE datasource='%s' \n"
        + "GROUP BY 1",
        testDataSourceName
    );

    String result = cluster.callApi().runSql(query);
    Assertions.assertEquals(StringUtils.format("%s,10", testDataSourceName), result);
  }

  @Test
  public void testSystemTableQueries_serverTypes()
  {
    String query = "SELECT server_type FROM sys.servers WHERE tier IS NOT NULL AND server_type <> 'indexer'";
    Assertions.assertEquals("historical", cluster.callApi().runSql(query));
  }

  /**
   * The console's {@code GROUP BY datasource} aggregate (no WHERE) is served by the broker's
   * SegmentsRollup, while the same aggregate with a {@code WHERE datasource=...} predicate falls back
   * to the normal segment scan (the predicate inserts a Filter the rollup rule doesn't match). Both
   * must return byte-identical results - this is the end-to-end guard that the rollup reproduces the
   * scan's semantics, including {@code AVG(num_rows)} being a DOUBLE rather than truncated integer.
   */
  @Test
  public void testSegmentsRollup_matchesScanForDashboardQuery()
  {
    // Ensure at least one rollup poll has completed after this test's segments became available, so
    // the accelerated path serves a fresh snapshot.
    broker.latchableEmitter().waitForNextEvent(event -> event.hasMetricName(ROLLUP_SYNC_METRIC));

    final String rollupResult = cluster.callApi().runSql(
        StringUtils.format(
            "SELECT datasource, %s FROM sys.segments GROUP BY datasource ORDER BY datasource",
            DASHBOARD_AGGREGATES
        )
    );

    final String scanResult = cluster.callApi().runSql(
        StringUtils.format(
            "SELECT datasource, %s FROM sys.segments WHERE datasource = '%s' GROUP BY datasource ORDER BY datasource",
            DASHBOARD_AGGREGATES,
            testDataSourceName
        )
    );

    // Only this test's datasource exists in this fresh cluster, so the unfiltered rollup result is the
    // single row for it, matching the datasource-filtered scan result exactly.
    Assertions.assertEquals(scanResult, rollupResult);
    Assertions.assertTrue(
        rollupResult.startsWith(testDataSourceName + ","),
        "unexpected rollup result: " + rollupResult
    );
  }

  /**
   * Confirms the acceleration actually engages (not that both paths merely agree): the unfiltered
   * aggregate is served by the rollup - emitting {@link #ROLLUP_QUERY_METRIC} - while the
   * datasource-filtered form inserts a Filter the rule can't match and falls back to a segment scan,
   * emitting nothing.
   */
  @Test
  public void testSegmentsRollup_servesOnlyTheUnfilteredQuery()
  {
    broker.latchableEmitter().waitForNextEvent(event -> event.hasMetricName(ROLLUP_SYNC_METRIC));

    final long beforeUnfiltered = broker.latchableEmitter().getMetricEventLongSum(ROLLUP_QUERY_METRIC);
    cluster.callApi().runSql(
        StringUtils.format(
            "SELECT datasource, %s FROM sys.segments GROUP BY datasource ORDER BY datasource",
            DASHBOARD_AGGREGATES
        )
    );
    final long afterUnfiltered = broker.latchableEmitter().getMetricEventLongSum(ROLLUP_QUERY_METRIC);
    Assertions.assertTrue(
        afterUnfiltered > beforeUnfiltered,
        "expected the unfiltered GROUP BY datasource query to be served by the rollup"
    );

    cluster.callApi().runSql(
        StringUtils.format(
            "SELECT datasource, %s FROM sys.segments WHERE datasource = '%s' GROUP BY datasource ORDER BY datasource",
            DASHBOARD_AGGREGATES,
            testDataSourceName
        )
    );
    Assertions.assertEquals(
        afterUnfiltered,
        broker.latchableEmitter().getMetricEventLongSum(ROLLUP_QUERY_METRIC),
        "datasource-filtered query must fall back to a scan, not use the rollup"
    );
  }
}
