/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class QueryContextsTest
{

  @Test
  public void testDefaultQueryTimeout()
  {
    final Query<?> query = new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(ImmutableList.of(new Interval("0/100"))),
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
        new MultipleIntervalSegmentSpec(ImmutableList.of(new Interval("0/100"))),
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
        new MultipleIntervalSegmentSpec(ImmutableList.of(new Interval("0/100"))),
        false,
        ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1000)
    );
    Assert.assertEquals(1000, QueryContexts.getTimeout(query));

    query = QueryContexts.withDefaultTimeout(query, 1_000_000);
    Assert.assertEquals(1000, QueryContexts.getTimeout(query));
  }
}
