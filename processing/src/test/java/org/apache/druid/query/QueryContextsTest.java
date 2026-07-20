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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class QueryContextsTest
{
  @Test
  public void testDefaultQueryTimeout()
  {
    final Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        new HashMap<>()
    );
    Assertions.assertEquals(300_000, query.context().getDefaultTimeout());
  }

  @Test
  public void testEmptyQueryTimeout()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        new HashMap<>()
    );
    Assertions.assertEquals(300_000, query.context().getTimeout());

    query = Queries.withDefaultTimeout(query, 60_000);
    Assertions.assertEquals(60_000, query.context().getTimeout());
  }

  @Test
  public void testQueryTimeout()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1000)
    );
    Assertions.assertEquals(1000, query.context().getTimeout());

    query = Queries.withDefaultTimeout(query, 1_000_000);
    Assertions.assertEquals(1000, query.context().getTimeout());
  }

  @Test
  public void testQueryMaxTimeout()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1000)
    );

    BadQueryContextException ex = Assertions.assertThrows(
        BadQueryContextException.class,
        () -> query.context().verifyMaxQueryTimeout(100)
    );
    Assertions.assertTrue(ex.getMessage().contains("Configured timeout = 1000 is more than enforced limit of 100."));
  }

  @Test
  public void testMaxScatterGatherBytes()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        ImmutableMap.of(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, 1000)
    );

    BadQueryContextException ex = Assertions.assertThrows(
        BadQueryContextException.class,
        () -> Queries.withMaxScatterGatherBytes(query, 100)
    );
    Assertions.assertTrue(ex.getMessage().contains("Configured maxScatterGatherBytes = 1000 is more than enforced limit of 100."));
  }

  @Test
  public void testDisableSegmentPruning()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        ImmutableMap.of(QueryContexts.SECONDARY_PARTITION_PRUNING_KEY, false)
    );
    Assertions.assertFalse(query.context().isSecondaryPartitionPruningEnabled());
  }

  @Test
  public void testDefaultSegmentPruning()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        ImmutableMap.of()
    );
    Assertions.assertTrue(query.context().isSecondaryPartitionPruningEnabled());
  }

  @Test
  public void testDefaultInSubQueryThreshold()
  {
    Assertions.assertEquals(
        QueryContexts.DEFAULT_IN_SUB_QUERY_THRESHOLD,
        QueryContext.empty().getInSubQueryThreshold()
    );
  }

  @Test
  public void testDefaultPlanTimeBoundarySql()
  {
    Assertions.assertEquals(
        QueryContexts.DEFAULT_ENABLE_TIME_BOUNDARY_PLANNING,
        QueryContext.empty().isTimeBoundaryPlanningEnabled()
    );
  }

  @Test
  public void testDefaultCloneQueryMode()
  {
    Assertions.assertEquals(
        CloneQueryMode.EXCLUDECLONES,
        QueryContext.empty().getCloneQueryMode()
    );
  }

  @Test
  public void testExcludeSourceCloneQueryMode()
  {
    Assert.assertEquals(
        CloneQueryMode.EXCLUDESOURCE,
        QueryContext.of(ImmutableMap.of(QueryContexts.CLONE_QUERY_MODE, "excludeSource")).getCloneQueryMode()
    );
  }

  @Test
  public void testCatalogValidationEnabled()
  {
    Assertions.assertEquals(
        QueryContexts.DEFAULT_CATALOG_VALIDATION_ENABLED,
        QueryContext.empty().isCatalogValidationEnabled()
    );
    Assertions.assertTrue(QueryContext.of(ImmutableMap.of(
        QueryContexts.CATALOG_VALIDATION_ENABLED,
        true
    )).isCatalogValidationEnabled());
    Assertions.assertFalse(QueryContext.of(ImmutableMap.of(
        QueryContexts.CATALOG_VALIDATION_ENABLED,
        false
    )).isCatalogValidationEnabled());
  }

  @Test
  public void testGetEnableJoinLeftScanDirect()
  {
    Assertions.assertFalse(QueryContext.empty().getEnableJoinLeftScanDirect());
    Assertions.assertTrue(QueryContext.of(ImmutableMap.of(
        QueryContexts.SQL_JOIN_LEFT_SCAN_DIRECT,
        true
    )).getEnableJoinLeftScanDirect());
    Assertions.assertFalse(QueryContext.of(ImmutableMap.of(
        QueryContexts.SQL_JOIN_LEFT_SCAN_DIRECT,
        false
    )).getEnableJoinLeftScanDirect());
  }

  @Test
  public void testGetBrokerServiceName()
  {
    Map<String, Object> queryContext = new HashMap<>();
    Assertions.assertNull(QueryContext.of(queryContext).getBrokerServiceName());

    queryContext.put(QueryContexts.BROKER_SERVICE_NAME, "hotBroker");
    Assertions.assertEquals("hotBroker", QueryContext.of(queryContext).getBrokerServiceName());
  }

  @Test
  public void testGetBrokerServiceName_withNonStringValue()
  {
    Map<String, Object> queryContext = new HashMap<>();
    queryContext.put(QueryContexts.BROKER_SERVICE_NAME, 100);

    Assertions.assertThrows(BadQueryContextException.class, () -> QueryContext.of(queryContext).getBrokerServiceName());
  }

  @Test
  public void testGetTimeout_withNonNumericValue()
  {
    Map<String, Object> queryContext = new HashMap<>();
    queryContext.put(QueryContexts.TIMEOUT_KEY, "2000'");

    Assertions.assertThrows(
        BadQueryContextException.class,
        () -> new TestQuery(
            new TableDataSource("test"),
            new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
            queryContext
        ).context().getTimeout()
    );
  }

  @Test
  public void testGetAs()
  {
    Assertions.assertNull(QueryContexts.getAsString("foo", null, null));
    Assertions.assertEquals("default", QueryContexts.getAsString("foo", null, "default"));
    Assertions.assertEquals("value", QueryContexts.getAsString("foo", "value", "default"));
    try {
      QueryContexts.getAsString("foo", 10, null);
      Assertions.fail();
    }
    catch (BadQueryContextException e) {
      // Expected
    }

    Assertions.assertFalse(QueryContexts.getAsBoolean("foo", null, false));
    Assertions.assertTrue(QueryContexts.getAsBoolean("foo", null, true));
    Assertions.assertTrue(QueryContexts.getAsBoolean("foo", "true", false));
    Assertions.assertTrue(QueryContexts.getAsBoolean("foo", true, false));
    try {
      QueryContexts.getAsBoolean("foo", 10, false);
      Assertions.fail();
    }
    catch (BadQueryContextException e) {
      // Expected
    }

    Assertions.assertEquals(10, QueryContexts.getAsInt("foo", null, 10));
    Assertions.assertEquals(20, QueryContexts.getAsInt("foo", "20", 10));
    Assertions.assertEquals(20, QueryContexts.getAsInt("foo", 20, 10));
    Assertions.assertEquals(20, QueryContexts.getAsInt("foo", 20L, 10));
    Assertions.assertEquals(20, QueryContexts.getAsInt("foo", 20D, 10));
    try {
      QueryContexts.getAsInt("foo", true, 20);
      Assertions.fail();
    }
    catch (BadQueryContextException e) {
      // Expected
    }

    Assertions.assertEquals(10L, QueryContexts.getAsLong("foo", null, 10));
    Assertions.assertEquals(20L, QueryContexts.getAsLong("foo", "20", 10));
    Assertions.assertEquals(20L, QueryContexts.getAsLong("foo", 20, 10));
    Assertions.assertEquals(20L, QueryContexts.getAsLong("foo", 20L, 10));
    Assertions.assertEquals(20L, QueryContexts.getAsLong("foo", 20D, 10));
    try {
      QueryContexts.getAsLong("foo", true, 20);
      Assertions.fail();
    }
    catch (BadQueryContextException e) {
      // Expected
    }
  }

  @Test
  public void testGetAsHumanReadableBytes()
  {
    Assertions.assertEquals(
        new HumanReadableBytes("500M").getBytes(),
        QueryContexts.getAsHumanReadableBytes("maxOnDiskStorage", 500_000_000, HumanReadableBytes.ZERO)
                     .getBytes()
    );
    Assertions.assertEquals(
        new HumanReadableBytes("500M").getBytes(),
        QueryContexts.getAsHumanReadableBytes("maxOnDiskStorage", "500000000", HumanReadableBytes.ZERO)
                     .getBytes()
    );
    Assertions.assertEquals(
        new HumanReadableBytes("500M").getBytes(),
        QueryContexts.getAsHumanReadableBytes("maxOnDiskStorage", "500M", HumanReadableBytes.ZERO)
                     .getBytes()
    );
  }

  @Test
  public void testGetEnum()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        ImmutableMap.of("e1", "FORCE",
                        "e2", "INVALID_ENUM"
        )
    );

    Assertions.assertEquals(
        QueryContexts.Vectorize.FORCE,
        query.context().getEnum("e1", QueryContexts.Vectorize.class, QueryContexts.Vectorize.FALSE)
    );

    Assertions.assertThrows(
        BadQueryContextException.class,
        () -> query.context().getEnum("e2", QueryContexts.Vectorize.class, QueryContexts.Vectorize.FALSE)
    );
  }

  @Test
  public void testExecutionModeEnum()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        ImmutableMap.of(QueryContexts.CTX_EXECUTION_MODE, "SYNC", QueryContexts.CTX_EXECUTION_MODE + "_1", "ASYNC")
    );

    Assertions.assertEquals(
        ExecutionMode.SYNC,
        query.context().getEnum(QueryContexts.CTX_EXECUTION_MODE, ExecutionMode.class, ExecutionMode.ASYNC)
    );

    Assertions.assertEquals(
        ExecutionMode.ASYNC,
        query.context().getEnum(QueryContexts.CTX_EXECUTION_MODE + "_1", ExecutionMode.class, ExecutionMode.SYNC)
    );
  }

}
