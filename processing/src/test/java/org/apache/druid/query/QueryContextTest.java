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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class QueryContextTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.configure()
                  .suppress(Warning.NONFINAL_FIELDS, Warning.ALL_FIELDS_SHOULD_BE_USED)
                  .usingGetClass()
                  .forClass(QueryContext.class)
                  .withNonnullFields("context")
                  .verify();
  }

  @Test
  public void testEmptyParam()
  {
    final QueryContext context = QueryContext.empty();
    Assert.assertEquals(ImmutableMap.of(), context.getContext());
  }

  @Test
  public void testIsEmpty()
  {
    Assert.assertTrue(QueryContext.empty().isEmpty());
    Assert.assertFalse(QueryContext.of(ImmutableMap.of("k", "v")).isEmpty());
  }

  @Test
  public void testGetString()
  {
    final QueryContext context = QueryContext.of(
        ImmutableMap.of("key", "val",
                        "key2", 2)
    );

    Assert.assertEquals("val", context.get("key"));
    Assert.assertEquals("val", context.getString("key"));
    Assert.assertNull(context.getString("non-exist"));
    Assert.assertEquals("foo", context.getString("non-exist", "foo"));

    Assert.assertThrows(BadQueryContextException.class, () -> context.getString("key2"));
  }

  @Test
  public void testGetBoolean()
  {
    final QueryContext context = QueryContext.of(
        ImmutableMap.of(
            "key1", "true",
            "key2", true
        )
    );

    Assert.assertTrue(context.getBoolean("key1", false));
    Assert.assertTrue(context.getBoolean("key2", false));
    Assert.assertTrue(context.getBoolean("key1"));
    Assert.assertFalse(context.getBoolean("non-exist", false));
    Assert.assertNull(context.getBoolean("non-exist"));
  }

  @Test
  public void testGetInt()
  {
    final QueryContext context = QueryContext.of(
        ImmutableMap.of(
            "key1", "100",
            "key2", 100,
            "key3", "abc"
        )
    );

    Assert.assertEquals(100, context.getInt("key1", 0));
    Assert.assertEquals(100, context.getInt("key2", 0));
    Assert.assertEquals(0, context.getInt("non-exist", 0));

    Assert.assertThrows(BadQueryContextException.class, () -> context.getInt("key3", 5));
  }

  @Test
  public void testGetLong()
  {
    final QueryContext context = QueryContext.of(
        ImmutableMap.of(
            "key1", "100",
            "key2", 100,
            "key3", "abc"
        )
    );

    Assert.assertEquals(100L, context.getLong("key1", 0));
    Assert.assertEquals(100L, context.getLong("key2", 0));
    Assert.assertEquals(0L, context.getLong("non-exist", 0));

    Assert.assertThrows(BadQueryContextException.class, () -> context.getLong("key3", 5));
  }

  @Test
  public void testGetFloat()
  {
    final QueryContext context = QueryContext.of(
        ImmutableMap.of(
            "f1", "500",
            "f2", 500,
            "f3", 500.1,
            "f4", "ab"
        )
    );

    Assert.assertEquals(0, Float.compare(500, context.getFloat("f1", 100)));
    Assert.assertEquals(0, Float.compare(500, context.getFloat("f2", 100)));
    Assert.assertEquals(0, Float.compare(500.1f, context.getFloat("f3", 100)));

    Assert.assertThrows(BadQueryContextException.class, () -> context.getFloat("f4", 5));
  }

  @Test
  public void testGetHumanReadableBytes()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.<String, Object>builder()
                    .put("m1", 500_000_000)
                    .put("m2", "500M")
                    .put("m3", "500Mi")
                    .put("m4", "500MiB")
                    .put("m5", "500000000")
                    .put("m6", "abc")
                    .build()
    );
    Assert.assertEquals(500_000_000, context.getHumanReadableBytes("m1", HumanReadableBytes.ZERO).getBytes());
    Assert.assertEquals(500_000_000, context.getHumanReadableBytes("m2", HumanReadableBytes.ZERO).getBytes());
    Assert.assertEquals(500 * 1024 * 1024L, context.getHumanReadableBytes("m3", HumanReadableBytes.ZERO).getBytes());
    Assert.assertEquals(500 * 1024 * 1024L, context.getHumanReadableBytes("m4", HumanReadableBytes.ZERO).getBytes());
    Assert.assertEquals(500_000_000, context.getHumanReadableBytes("m5", HumanReadableBytes.ZERO).getBytes());

    Assert.assertThrows(BadQueryContextException.class, () -> context.getHumanReadableBytes("m6", HumanReadableBytes.ZERO));
  }

  @Test
  public void testDefaultEnableQueryDebugging()
  {
    Assert.assertFalse(QueryContext.empty().isDebug());
    Assert.assertTrue(QueryContext.of(ImmutableMap.of(QueryContexts.ENABLE_DEBUG, true)).isDebug());
  }

  // This test is a bit silly. It is retained because another test uses the
  // LegacyContextQuery test.
  @Test
  public void testLegacyReturnsLegacy()
  {
    Map<String, Object> context = ImmutableMap.of("foo", "bar");
    Query<?> legacy = new LegacyContextQuery(context);
    Assert.assertEquals(context, legacy.getContext());
  }

  @Test
  public void testNonLegacyIsNotLegacyContext()
  {
    Query<?> timeseries = Druids.newTimeseriesQueryBuilder()
                                .dataSource("test")
                                .intervals("2015-01-02/2015-01-03")
                                .granularity(Granularities.DAY)
                                .aggregators(Collections.singletonList(new CountAggregatorFactory("theCount")))
                                .context(ImmutableMap.of("foo", "bar"))
                                .build();
    Assert.assertNotNull(timeseries.getContext());
  }

  public static class LegacyContextQuery implements Query<Integer>
  {
    private final Map<String, Object> context;

    public LegacyContextQuery(Map<String, Object> context)
    {
      this.context = context;
    }

    @Override
    public DataSource getDataSource()
    {
      return new TableDataSource("fake");
    }

    @Override
    public boolean hasFilters()
    {
      return false;
    }

    @Override
    public DimFilter getFilter()
    {
      return null;
    }

    @Override
    public String getType()
    {
      return "legacy-context-query";
    }

    @Override
    public QueryRunner<Integer> getRunner(QuerySegmentWalker walker)
    {
      return new NoopQueryRunner<>();
    }

    @Override
    public List<Interval> getIntervals()
    {
      return Collections.singletonList(Intervals.ETERNITY);
    }

    @Override
    public Duration getDuration()
    {
      return getIntervals().get(0).toDuration();
    }

    @Override
    public Granularity getGranularity()
    {
      return Granularities.ALL;
    }

    @Override
    public DateTimeZone getTimezone()
    {
      return DateTimeZone.UTC;
    }

    @Override
    public Map<String, Object> getContext()
    {
      return context;
    }

    @Override
    public boolean isDescending()
    {
      return false;
    }

    @Override
    public Ordering<Integer> getResultOrdering()
    {
      return Ordering.natural();
    }

    @Override
    public Query<Integer> withQuerySegmentSpec(QuerySegmentSpec spec)
    {
      return new LegacyContextQuery(context);
    }

    @Override
    public Query<Integer> withId(String id)
    {
      context.put(BaseQuery.QUERY_ID, id);
      return this;
    }

    @Nullable
    @Override
    public String getId()
    {
      return (String) context.get(BaseQuery.QUERY_ID);
    }

    @Override
    public Query<Integer> withSubQueryId(String subQueryId)
    {
      context.put(BaseQuery.SUB_QUERY_ID, subQueryId);
      return this;
    }

    @Nullable
    @Override
    public String getSubQueryId()
    {
      return (String) context.get(BaseQuery.SUB_QUERY_ID);
    }

    @Override
    public Query<Integer> withDataSource(DataSource dataSource)
    {
      return this;
    }

    @Override
    public Query<Integer> withOverriddenContext(Map<String, Object> contextOverride)
    {
      return new LegacyContextQuery(contextOverride);
    }
  }
}
