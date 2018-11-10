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
}
