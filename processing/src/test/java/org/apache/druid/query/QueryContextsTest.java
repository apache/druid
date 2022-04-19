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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

public class QueryContextsTest
{
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testDefaultQueryTimeout()
  {
    final Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        false,
        new HashMap()
    );
    Assert.assertEquals(300_000, QueryContexts.getDefaultTimeout(query));
  }

  @Test
  public void testEmptyQueryTimeout()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        false,
        new HashMap()
    );
    Assert.assertEquals(300_000, QueryContexts.getTimeout(query));

    query = QueryContexts.withDefaultTimeout(query, 60_000);
    Assert.assertEquals(60_000, QueryContexts.getTimeout(query));
  }

  @Test
  public void testQueryTimeout()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        false,
        ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1000)
    );
    Assert.assertEquals(1000, QueryContexts.getTimeout(query));

    query = QueryContexts.withDefaultTimeout(query, 1_000_000);
    Assert.assertEquals(1000, QueryContexts.getTimeout(query));
  }

  @Test
  public void testQueryMaxTimeout()
  {
    exception.expect(IAE.class);
    exception.expectMessage("configured [timeout = 1000] is more than enforced limit of maxQueryTimeout [100].");
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        false,
        ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1000)
    );

    QueryContexts.verifyMaxQueryTimeout(query, 100);
  }

  @Test
  public void testMaxScatterGatherBytes()
  {
    exception.expect(IAE.class);
    exception.expectMessage("configured [maxScatterGatherBytes = 1000] is more than enforced limit of [100].");
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        false,
        ImmutableMap.of(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, 1000)
    );

    QueryContexts.withMaxScatterGatherBytes(query, 100);
  }

  @Test
  public void testDisableSegmentPruning()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        false,
        ImmutableMap.of(QueryContexts.SECONDARY_PARTITION_PRUNING_KEY, false)
    );
    Assert.assertFalse(QueryContexts.isSecondaryPartitionPruningEnabled(query));
  }

  @Test
  public void testDefaultSegmentPruning()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        false,
        ImmutableMap.of()
    );
    Assert.assertTrue(QueryContexts.isSecondaryPartitionPruningEnabled(query));
  }

  @Test
  public void testDefaultInSubQueryThreshold()
  {
    Assert.assertEquals(QueryContexts.DEFAULT_IN_SUB_QUERY_THRESHOLD,
                        QueryContexts.getInSubQueryThreshold(ImmutableMap.of()));
  }

  @Test
  public void testGetEnableJoinLeftScanDirect()
  {
    Assert.assertFalse(QueryContexts.getEnableJoinLeftScanDirect(ImmutableMap.of()));
    Assert.assertTrue(QueryContexts.getEnableJoinLeftScanDirect(ImmutableMap.of(
        QueryContexts.SQL_JOIN_LEFT_SCAN_DIRECT,
        true
    )));
    Assert.assertFalse(QueryContexts.getEnableJoinLeftScanDirect(ImmutableMap.of(
        QueryContexts.SQL_JOIN_LEFT_SCAN_DIRECT,
        false
    )));
  }

  @Test
  public void testGetBrokerServiceName()
  {
    Map<String, Object> queryContext = new HashMap<>();
    Assert.assertNull(QueryContexts.getBrokerServiceName(queryContext));

    queryContext.put(QueryContexts.BROKER_SERVICE_NAME, "hotBroker");
    Assert.assertEquals("hotBroker", QueryContexts.getBrokerServiceName(queryContext));
  }

  @Test
  public void testGetBrokerServiceName_withNonStringValue()
  {
    Map<String, Object> queryContext = new HashMap<>();
    queryContext.put(QueryContexts.BROKER_SERVICE_NAME, 100);

    exception.expect(ClassCastException.class);
    QueryContexts.getBrokerServiceName(queryContext);
  }

  @Test
  public void testDefaultEnableQueryDebugging()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        false,
        ImmutableMap.of()
    );
    Assert.assertFalse(QueryContexts.isDebug(query));
    Assert.assertFalse(QueryContexts.isDebug(query.getContext()));
  }

  @Test
  public void testEnableQueryDebuggingSetToTrue()
  {
    Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("0/100"))),
        false,
        ImmutableMap.of(QueryContexts.ENABLE_DEBUG, true)
    );
    Assert.assertTrue(QueryContexts.isDebug(query));
    Assert.assertTrue(QueryContexts.isDebug(query.getContext()));
  }
}
