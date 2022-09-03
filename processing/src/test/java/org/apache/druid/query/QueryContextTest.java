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
import org.apache.druid.java.util.common.IAE;
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
                  .withNonnullFields("defaultParams", "userParams", "systemParams")
                  .verify();
  }

  @Test
  public void testEmptyParam()
  {
    final QueryContext context = new QueryContext();
    Assert.assertEquals(ImmutableMap.of(), context.getMergedParams());
  }

  @Test
  public void testIsEmpty()
  {
    Assert.assertTrue(new QueryContext().isEmpty());
    Assert.assertFalse(new QueryContext(ImmutableMap.of("k", "v")).isEmpty());
    QueryContext context = new QueryContext();
    context.addDefaultParam("k", "v");
    Assert.assertFalse(context.isEmpty());
    context = new QueryContext();
    context.addSystemParam("k", "v");
    Assert.assertFalse(context.isEmpty());
  }

  @Test
  public void testGetString()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of("key", "val",
                        "key2", 2)
    );

    Assert.assertEquals("val", context.get("key"));
    Assert.assertEquals("val", context.getAsString("key"));
    Assert.assertEquals("2", context.getAsString("key2"));
    Assert.assertNull(context.getAsString("non-exist"));
  }

  @Test
  public void testGetBoolean()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "key1", "true",
            "key2", true
        )
    );

    Assert.assertTrue(context.getAsBoolean("key1", false));
    Assert.assertTrue(context.getAsBoolean("key2", false));
    Assert.assertFalse(context.getAsBoolean("non-exist", false));
  }

  @Test
  public void testGetInt()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "key1", "100",
            "key2", 100,
            "key3", "abc"
        )
    );

    Assert.assertEquals(100, context.getAsInt("key1", 0));
    Assert.assertEquals(100, context.getAsInt("key2", 0));
    Assert.assertEquals(0, context.getAsInt("non-exist", 0));

    Assert.assertThrows(IAE.class, () -> context.getAsInt("key3", 5));
  }

  @Test
  public void testGetLong()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "key1", "100",
            "key2", 100,
            "key3", "abc"
        )
    );

    Assert.assertEquals(100L, context.getAsLong("key1", 0));
    Assert.assertEquals(100L, context.getAsLong("key2", 0));
    Assert.assertEquals(0L, context.getAsLong("non-exist", 0));

    Assert.assertThrows(IAE.class, () -> context.getAsLong("key3", 5));
  }

  @Test
  public void testGetFloat()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "f1", "500",
            "f2", 500,
            "f3", 500.1,
            "f4", "ab"
        )
    );

    Assert.assertEquals(0, Float.compare(500, context.getAsFloat("f1", 100)));
    Assert.assertEquals(0, Float.compare(500, context.getAsFloat("f2", 100)));
    Assert.assertEquals(0, Float.compare(500.1f, context.getAsFloat("f3", 100)));

    Assert.assertThrows(IAE.class, () -> context.getAsLong("f4", 5));
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
    Assert.assertEquals(500_000_000, context.getAsHumanReadableBytes("m1", HumanReadableBytes.ZERO).getBytes());
    Assert.assertEquals(500_000_000, context.getAsHumanReadableBytes("m2", HumanReadableBytes.ZERO).getBytes());
    Assert.assertEquals(500 * 1024 * 1024L, context.getAsHumanReadableBytes("m3", HumanReadableBytes.ZERO).getBytes());
    Assert.assertEquals(500 * 1024 * 1024L, context.getAsHumanReadableBytes("m4", HumanReadableBytes.ZERO).getBytes());
    Assert.assertEquals(500_000_000, context.getAsHumanReadableBytes("m5", HumanReadableBytes.ZERO).getBytes());

    Assert.assertThrows(IAE.class, () -> context.getAsHumanReadableBytes("m6", HumanReadableBytes.ZERO));
  }

  @Test
  public void testAddSystemParamOverrideUserParam()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        )
    );
    context.addSystemParam("sys1", "sysVal1");
    context.addSystemParam("conflict", "sysVal2");

    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        ),
        context.getUserParams()
    );

    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "sys1", "sysVal1",
            "conflict", "sysVal2"
        ),
        context.getMergedParams()
    );
  }

  @Test
  public void testUserParamOverrideDefaultParam()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        )
    );
    context.addDefaultParams(
        ImmutableMap.of(
            "default1", "defaultVal1"
        )
    );
    context.addDefaultParam("conflict", "defaultVal2");

    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        ),
        context.getUserParams()
    );

    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "default1", "defaultVal1",
            "conflict", "userVal2"
        ),
        context.getMergedParams()
    );
  }

  @Test
  public void testRemoveUserParam()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        )
    );
    context.addDefaultParams(
        ImmutableMap.of(
            "default1", "defaultVal1",
            "conflict", "defaultVal2"
        )
    );

    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "default1", "defaultVal1",
            "conflict", "userVal2"
        ),
        context.getMergedParams()
    );
    Assert.assertEquals("userVal2", context.removeUserParam("conflict"));
    Assert.assertEquals(
        ImmutableMap.of(
            "user1", "userVal1",
            "default1", "defaultVal1",
            "conflict", "defaultVal2"
        ),
        context.getMergedParams()
    );
  }

  @Test
  public void testGetMergedParams()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        )
    );
    context.addDefaultParams(
        ImmutableMap.of(
            "default1", "defaultVal1",
            "conflict", "defaultVal2"
        )
    );

    Assert.assertSame(context.getMergedParams(), context.getMergedParams());
  }

  @Test
  public void testCopy()
  {
    final QueryContext context = new QueryContext(
        ImmutableMap.of(
            "user1", "userVal1",
            "conflict", "userVal2"
        )
    );

    context.addDefaultParams(
        ImmutableMap.of(
            "default1", "defaultVal1",
            "conflict", "defaultVal2"
        )
    );

    context.addSystemParam("sys1", "val1");

    final Map<String, Object> merged = ImmutableMap.copyOf(context.getMergedParams());

    final QueryContext context2 = context.copy();
    context2.removeUserParam("conflict");
    context2.addSystemParam("sys2", "val2");
    context2.addDefaultParam("default3", "defaultVal3");

    Assert.assertEquals(merged, context.getMergedParams());
  }

  @Test
  public void testLegacyReturnsLegacy()
  {
    Query<?> legacy = new LegacyContextQuery(ImmutableMap.of("foo", "bar"));
    Assert.assertNull(legacy.getQueryContext());
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
    Assert.assertNotNull(timeseries.getQueryContext());
  }

  public static class LegacyContextQuery implements Query
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
    public QueryRunner getRunner(QuerySegmentWalker walker)
    {
      return new NoopQueryRunner();
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
    public boolean getContextBoolean(String key, boolean defaultValue)
    {
      if (context == null || !context.containsKey(key)) {
        return defaultValue;
      }
      return (boolean) context.get(key);
    }

    @Override
    public HumanReadableBytes getContextAsHumanReadableBytes(String key, HumanReadableBytes defaultValue)
    {
      if (null == context || !context.containsKey(key)) {
        return defaultValue;
      }
      Object value = context.get(key);
      if (value instanceof Number) {
        return HumanReadableBytes.valueOf(((Number) value).longValue());
      } else if (value instanceof String) {
        return new HumanReadableBytes((String) value);
      } else {
        throw new IAE("Expected parameter [%s] to be in human readable format", key);
      }
    }

    @Override
    public boolean isDescending()
    {
      return false;
    }

    @Override
    public Ordering getResultOrdering()
    {
      return Ordering.natural();
    }

    @Override
    public Query withQuerySegmentSpec(QuerySegmentSpec spec)
    {
      return new LegacyContextQuery(context);
    }

    @Override
    public Query withId(String id)
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
    public Query withSubQueryId(String subQueryId)
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
    public Query withDataSource(DataSource dataSource)
    {
      return this;
    }

    @Override
    public Query withOverriddenContext(Map contextOverride)
    {
      return new LegacyContextQuery(contextOverride);
    }

    @Override
    public Object getContextValue(String key)
    {
      return context.get(key);
    }
  }
}
