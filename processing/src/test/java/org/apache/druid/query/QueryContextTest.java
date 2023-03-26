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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
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
import org.apache.druid.segment.DimensionHandlerUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QueryContextTest
{
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

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

  /**
   * Verify that a context with an null map is the same as a context with
   * an empty map.
   */
  @Test
  public void testEmptyContext()
  {
    {
      final QueryContext context = new QueryContext(null);
      assertEquals(ImmutableMap.of(), context.asMap());
    }
    {
      final QueryContext context = new QueryContext(new HashMap<>());
      assertEquals(ImmutableMap.of(), context.asMap());
    }
    {
      final QueryContext context = QueryContext.of(null);
      assertEquals(ImmutableMap.of(), context.asMap());
    }
    {
      final QueryContext context = QueryContext.of(new HashMap<>());
      assertEquals(ImmutableMap.of(), context.asMap());
    }
    {
      final QueryContext context = QueryContext.empty();
      assertEquals(ImmutableMap.of(), context.asMap());
    }
  }

  @Test
  public void testIsEmpty()
  {
    assertTrue(QueryContext.empty().isEmpty());
    assertFalse(QueryContext.of(ImmutableMap.of("k", "v")).isEmpty());
  }

  @Test
  public void testGetString()
  {
    final QueryContext context = QueryContext.of(
        ImmutableMap.of("key", "val",
                        "key2", 2)
    );

    assertEquals("val", context.get("key"));
    assertEquals("val", context.getString("key"));
    assertNull(context.getString("non-exist"));
    assertEquals("foo", context.getString("non-exist", "foo"));

    assertThrows(BadQueryContextException.class, () -> context.getString("key2"));
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

    assertTrue(context.getBoolean("key1", false));
    assertTrue(context.getBoolean("key2", false));
    assertTrue(context.getBoolean("key1"));
    assertFalse(context.getBoolean("non-exist", false));
    assertNull(context.getBoolean("non-exist"));
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

    assertEquals(100, context.getInt("key1", 0));
    assertEquals(100, context.getInt("key2", 0));
    assertEquals(0, context.getInt("non-exist", 0));

    assertThrows(BadQueryContextException.class, () -> context.getInt("key3", 5));
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

    assertEquals(100L, context.getLong("key1", 0));
    assertEquals(100L, context.getLong("key2", 0));
    assertEquals(0L, context.getLong("non-exist", 0));

    assertThrows(BadQueryContextException.class, () -> context.getLong("key3", 5));
  }

  /**
   * Tests the several ways that Druid code parses context strings into Long
   * values. The desired behavior is that "x" is parsed exactly the same as Jackson
   * would parse x (where x is a valid number.) The context methods must emulate
   * Jackson. The dimension utility method is included because some code used that
   * for long parsing, and we must maintain backward compatibility.
   * <p>
   * The exceptions in the {@code assertThrows} are not critical: the key thing is
   * that we're documenting what works and what doesn't. If an exception changes,
   * just update the tests. If something no longer throws an exception, we'll want
   * to verify that we support the new use case consistently in all three paths.
   */
  @Test
  public void testGetLongCompatibility() throws JsonProcessingException
  {
    {
      String value = null;

      // Only the context methods allow {"foo": null} to be parsed as a null Long.
      assertNull(getContextLong(value));
      // Nulls not legal on this path.
      assertThrows(NullPointerException.class, () -> getDimensionLong(value));
      // Nulls not legal on this path.
      assertThrows(IllegalArgumentException.class, () -> getJsonLong(value));
    }

    {
      String value = "";
      // Blank string not legal on this path.
      assertThrows(BadQueryContextException.class, () -> getContextLong(value));
      assertNull(getDimensionLong(value));
      // Blank string not allowed where a value is expected.
      assertThrows(MismatchedInputException.class, () -> getJsonLong(value));
    }

    {
      String value = "0";
      assertEquals(0L, (long) getContextLong(value));
      assertEquals(0L, (long) getDimensionLong(value));
      assertEquals(0L, (long) getJsonLong(value));
    }

    {
      String value = "+1";
      assertEquals(1L, (long) getContextLong(value));
      assertEquals(1L, (long) getDimensionLong(value));
      assertThrows(JsonParseException.class, () -> getJsonLong(value));
    }

    {
      String value = "-1";
      assertEquals(-1L, (long) getContextLong(value));
      assertEquals(-1L, (long) getDimensionLong(value));
      assertEquals(-1L, (long) getJsonLong(value));
    }

    {
      // Hexadecimal numbers are not supported in JSON. Druid also does not support
      // them in strings.
      String value = "0xabcd";
      assertThrows(BadQueryContextException.class, () -> getContextLong(value));
      // The dimension utils have a funny way of handling hex: they return null
      assertNull(getDimensionLong(value));
      assertThrows(JsonParseException.class, () -> getJsonLong(value));
    }

    {
      // Leading zeros supported by Druid parsing, but not by JSON.
      String value = "05";
      assertEquals(5L, (long) getContextLong(value));
      assertEquals(5L, (long) getDimensionLong(value));
      assertThrows(JsonParseException.class, () -> getJsonLong(value));
    }

    {
      // The dimension utils allow a float where a long is expected.
      // Jackson can do this conversion. This test verifies that the context
      // functions can handle the same conversion.
      String value = "10.00";
      assertEquals(10L, (long) getContextLong(value));
      assertEquals(10L, (long) getDimensionLong(value));
      assertEquals(10L, (long) getJsonLong(value));
    }

    {
      // None of the conversion methods allow a (thousands) separator. The comma
      // would be ambiguous in JSON. Java allows the underscore, but JSON does
      // not support this syntax, and neither does Druid's string-to-long conversion.
      String value = "1_234";
      assertThrows(BadQueryContextException.class, () -> getContextLong(value));
      assertNull(getDimensionLong(value));
      assertThrows(JsonParseException.class, () -> getJsonLong(value));
    }
  }

  private static Long getContextLong(String value)
  {
    return QueryContexts.getAsLong("dummy", value);
  }

  private static Long getJsonLong(String value) throws JsonProcessingException
  {
    return JSON_MAPPER.readValue(value, Long.class);
  }

  private static Long getDimensionLong(String value)
  {
    return DimensionHandlerUtils.getExactLongFromDecimalString(value);
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

    assertEquals(0, Float.compare(500, context.getFloat("f1", 100)));
    assertEquals(0, Float.compare(500, context.getFloat("f2", 100)));
    assertEquals(0, Float.compare(500.1f, context.getFloat("f3", 100)));

    assertThrows(BadQueryContextException.class, () -> context.getFloat("f4", 5));
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
    assertEquals(500_000_000, context.getHumanReadableBytes("m1", HumanReadableBytes.ZERO).getBytes());
    assertEquals(500_000_000, context.getHumanReadableBytes("m2", HumanReadableBytes.ZERO).getBytes());
    assertEquals(500 * 1024 * 1024L, context.getHumanReadableBytes("m3", HumanReadableBytes.ZERO).getBytes());
    assertEquals(500 * 1024 * 1024L, context.getHumanReadableBytes("m4", HumanReadableBytes.ZERO).getBytes());
    assertEquals(500_000_000, context.getHumanReadableBytes("m5", HumanReadableBytes.ZERO).getBytes());

    assertThrows(BadQueryContextException.class, () -> context.getHumanReadableBytes("m6", HumanReadableBytes.ZERO));
  }

  @Test
  public void testDefaultEnableQueryDebugging()
  {
    assertFalse(QueryContext.empty().isDebug());
    assertTrue(QueryContext.of(ImmutableMap.of(QueryContexts.ENABLE_DEBUG, true)).isDebug());
  }

  // This test is a bit silly. It is retained because another test uses the
  // LegacyContextQuery test.
  @Test
  public void testLegacyReturnsLegacy()
  {
    Map<String, Object> context = ImmutableMap.of("foo", "bar");
    Query<?> legacy = new LegacyContextQuery(context);
    assertEquals(context, legacy.getContext());
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
    assertNotNull(timeseries.getContext());
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
