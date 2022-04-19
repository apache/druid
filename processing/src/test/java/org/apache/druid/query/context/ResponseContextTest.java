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

package org.apache.druid.query.context;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext.CounterKey;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.query.context.ResponseContext.Keys;
import org.apache.druid.query.context.ResponseContext.StringKey;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ResponseContextTest
{
  // Droppable header key
  static final Key EXTN_STRING_KEY = new StringKey(
      "extn_string_key", true, true);
  // Non-droppable header key
  static final Key EXTN_COUNTER_KEY = new CounterKey(
      "extn_counter_key", true);

  static {
    Keys.instance().registerKeys(new Key[] {
        EXTN_STRING_KEY,
        EXTN_COUNTER_KEY
    });
  }

  static final Key UNREGISTERED_KEY = new StringKey(
      "unregistered-key", true, true);

  @Test(expected = IllegalStateException.class)
  public void putISETest()
  {
    ResponseContext.createEmpty().put(UNREGISTERED_KEY, new Object());
  }

  @Test(expected = IllegalStateException.class)
  public void addISETest()
  {
    ResponseContext.createEmpty().add(UNREGISTERED_KEY, new Object());
  }

  @Test(expected = IllegalArgumentException.class)
  public void registerKeyIAETest()
  {
    Keys.INSTANCE.registerKey(Keys.NUM_SCANNED_ROWS);
  }

  @Test
  public void mergeETagTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.putEntityTag("dummy-etag");
    Assert.assertEquals("dummy-etag", ctx.getEntityTag());
    ctx.putEntityTag("new-dummy-etag");
    Assert.assertEquals("new-dummy-etag", ctx.getEntityTag());
  }

  private static final Interval INTERVAL_01 = Intervals.of("2019-01-01/P1D");
  private static final Interval INTERVAL_12 = Intervals.of("2019-01-02/P1D");
  private static final Interval INTERVAL_23 = Intervals.of("2019-01-03/P1D");

  @Test
  public void mergeUncoveredIntervalsTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.putUncoveredIntervals(Collections.singletonList(INTERVAL_01), false);
    Assert.assertArrayEquals(
        Collections.singletonList(INTERVAL_01).toArray(),
        ctx.getUncoveredIntervals().toArray()
    );
    ctx.add(Keys.UNCOVERED_INTERVALS, Arrays.asList(INTERVAL_12, INTERVAL_23));
    Assert.assertArrayEquals(
        Arrays.asList(INTERVAL_01, INTERVAL_12, INTERVAL_23).toArray(),
        ctx.getUncoveredIntervals().toArray()
    );
  }

  @Test
  public void mergeRemainingResponseTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    final String queryId = "queryId";
    final String queryId2 = "queryId2";
    ctx.initialize();
    ctx.addRemainingResponse(queryId, 3);
    ctx.addRemainingResponse(queryId2, 4);
    ctx.addRemainingResponse(queryId, -1);
    ctx.addRemainingResponse(queryId, -2);
    Assert.assertEquals(
        ImmutableMap.of(queryId, 0, queryId2, 4),
        ctx.get(Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS)
    );
  }

  @Test
  public void mergeMissingSegmentsTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    final SegmentDescriptor sd01 = new SegmentDescriptor(INTERVAL_01, "01", 0);
    ctx.addMissingSegments(Collections.singletonList(sd01));
    Assert.assertArrayEquals(
        Collections.singletonList(sd01).toArray(),
        ctx.getMissingSegments().toArray()
    );
    final SegmentDescriptor sd12 = new SegmentDescriptor(INTERVAL_12, "12", 1);
    final SegmentDescriptor sd23 = new SegmentDescriptor(INTERVAL_23, "23", 2);
    ctx.addMissingSegments(Arrays.asList(sd12, sd23));
    Assert.assertArrayEquals(
        Arrays.asList(sd01, sd12, sd23).toArray(),
        ctx.getMissingSegments().toArray()
    );
  }

  @Test
  public void initScannedRowsTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    Assert.assertNull(ctx.getRowScanCount());
    ctx.initializeRowScanCount();
    Assert.assertEquals((Long) 0L, ctx.getRowScanCount());
  }

  @Test
  public void mergeScannedRowsTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    Assert.assertNull(ctx.getRowScanCount());
    ctx.addRowScanCount(0L);
    Assert.assertEquals((Long) 0L, ctx.getRowScanCount());
    ctx.addRowScanCount(1L);
    Assert.assertEquals((Long) 1L, ctx.getRowScanCount());
    ctx.addRowScanCount(3L);
    Assert.assertEquals((Long) 4L, ctx.getRowScanCount());
  }

  @Test
  public void mergeUncoveredIntervalsOverflowedTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.add(Keys.UNCOVERED_INTERVALS_OVERFLOWED, false);
    Assert.assertEquals(false, ctx.get(Keys.UNCOVERED_INTERVALS_OVERFLOWED));
    ctx.add(Keys.UNCOVERED_INTERVALS_OVERFLOWED, true);
    Assert.assertEquals(true, ctx.get(Keys.UNCOVERED_INTERVALS_OVERFLOWED));
    ctx.add(Keys.UNCOVERED_INTERVALS_OVERFLOWED, false);
    Assert.assertEquals(true, ctx.get(Keys.UNCOVERED_INTERVALS_OVERFLOWED));
  }

  @Test
  public void mergeResponseContextTest()
  {
    final ResponseContext ctx1 = ResponseContext.createEmpty();
    ctx1.putEntityTag("dummy-etag-1");
    ctx1.putUncoveredIntervals(Collections.singletonList(INTERVAL_01), false);
    ctx1.addRowScanCount(1L);

    final ResponseContext ctx2 = ResponseContext.createEmpty();
    ctx2.putEntityTag("dummy-etag-2");
    ctx2.putUncoveredIntervals(Collections.singletonList(INTERVAL_12), false);
    final SegmentDescriptor sd01 = new SegmentDescriptor(INTERVAL_01, "01", 0);
    ctx2.addMissingSegments(Collections.singletonList(sd01));
    ctx2.addRowScanCount(2L);

    ctx1.merge(ctx2);
    Assert.assertEquals("dummy-etag-2", ctx1.getEntityTag());
    Assert.assertEquals((Long) 3L, ctx1.getRowScanCount());
    Assert.assertArrayEquals(
        Arrays.asList(INTERVAL_01, INTERVAL_12).toArray(),
        ctx1.getUncoveredIntervals().toArray()
    );
    Assert.assertArrayEquals(
        Collections.singletonList(sd01).toArray(),
        ctx1.getMissingSegments().toArray()
    );
  }

  @Test(expected = IllegalStateException.class)
  public void mergeISETest()
  {
    final ResponseContext ctx = new ResponseContext()
    {
      @Override
      protected Map<Key, Object> getDelegate()
      {
        return ImmutableMap.of(UNREGISTERED_KEY, "non-registered-key");
      }
    };
    ResponseContext.createEmpty().merge(ctx);
  }

  @Test
  public void serializeWithCorrectnessTest() throws JsonProcessingException
  {
    final ResponseContext ctx1 = ResponseContext.createEmpty();
    ctx1.add(EXTN_STRING_KEY, "string-value");
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(
        mapper.writeValueAsString(ImmutableMap.of(
            EXTN_STRING_KEY.getName(),
            "string-value")),
        ctx1.serializeWith(mapper, Integer.MAX_VALUE).getResult());

    final ResponseContext ctx2 = ResponseContext.createEmpty();
    // Add two non-header fields, and one that will be in the header
    ctx2.putEntityTag("not in header");
    ctx2.addCpuNanos(100);
    ctx2.add(EXTN_COUNTER_KEY, 100);
    Assert.assertEquals(
        mapper.writeValueAsString(ImmutableMap.of(
            EXTN_COUNTER_KEY.getName(), 100)),
        ctx2.serializeWith(mapper, Integer.MAX_VALUE).getResult());
  }

  private Map<ResponseContext.Key, Object> deserializeContext(String input, ObjectMapper mapper) throws IOException
  {
    return ResponseContext.deserialize(input, mapper).getDelegate();
  }

  @Test
  public void serializeWithTruncateValueTest() throws IOException
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.put(EXTN_COUNTER_KEY, 100L);
    ctx.put(EXTN_STRING_KEY, "long-string-that-is-supposed-to-be-removed-from-result");
    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    final ResponseContext.SerializationResult res1 = ctx.serializeWith(objectMapper, Integer.MAX_VALUE);
    Assert.assertEquals(ctx.getDelegate(), deserializeContext(res1.getResult(), objectMapper));
    final ResponseContext ctxCopy = ResponseContext.createEmpty();
    ctxCopy.merge(ctx);
    final int target = EXTN_COUNTER_KEY.getName().length() + 3 +
                       Keys.TRUNCATED.getName().length() + 5 +
                       15; // Fudge factor for quotes, separators, etc.
    final ResponseContext.SerializationResult res2 = ctx.serializeWith(objectMapper, target);
    ctxCopy.remove(EXTN_STRING_KEY);
    ctxCopy.put(Keys.TRUNCATED, true);
    Assert.assertEquals(
        ctxCopy.getDelegate(),
        deserializeContext(res2.getResult(), objectMapper)
    );
  }

  /**
   * Tests the case in which the sender knows about a key that the
   * receiver does not know about. The receiver will silently ignore
   * such keys.
   * @throws IOException
   */
  @Test
  public void deserializeWithUnknownKeyTest() throws IOException
  {
    Map<String, Object> bogus = new HashMap<>();
    bogus.put(Keys.ETAG.getName(), "eTag");
    bogus.put("scalar", "doomed");
    bogus.put("array", new String[]{"foo", "bar"});
    Map<String, Object> objValue = new HashMap<>();
    objValue.put("array", new String[]{"foo", "bar"});
    bogus.put("obj", objValue);
    bogus.put("null", null);
    final ObjectMapper mapper = new DefaultObjectMapper();
    String serialized = mapper.writeValueAsString(bogus);
    ResponseContext ctx = ResponseContext.deserialize(serialized, mapper);
    Assert.assertEquals(1, ctx.getDelegate().size());
    Assert.assertEquals("eTag", ctx.get(Keys.ETAG));
  }

  // Interval value for the test. Must match the deserialized value.
  private static Interval interval(int n)
  {
    return Intervals.of(StringUtils.format("2021-01-%02d/PT1M", n));
  }

  // Length of above with quotes and comma.
  private static final int INTERVAL_LEN = 52;

  @Test
  public void serializeWithTruncateArrayTest() throws IOException
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.put(
        Keys.UNCOVERED_INTERVALS,
        Arrays.asList(interval(1), interval(2), interval(3), interval(4),
                      interval(5), interval(6))
    );
    // This value should be longer than the above so it is fully removed
    // before we truncate the above.
    ctx.put(
        EXTN_STRING_KEY,
        Strings.repeat("x", INTERVAL_LEN * 7)
    );
    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    final ResponseContext.SerializationResult res1 = ctx.serializeWith(objectMapper, Integer.MAX_VALUE);
    Assert.assertEquals(ctx.getDelegate(),
        deserializeContext(res1.getResult(), objectMapper)
    );
    final int maxLen = INTERVAL_LEN * 4 + Keys.UNCOVERED_INTERVALS.getName().length() + 4 +
                       Keys.TRUNCATED.getName().length() + 6;
    final ResponseContext.SerializationResult res2 = ctx.serializeWith(objectMapper, maxLen);
    final ResponseContext ctxCopy = ResponseContext.createEmpty();
    // The resulting key array length will be half the start
    // length.
    ctxCopy.put(Keys.UNCOVERED_INTERVALS, Arrays.asList(interval(1), interval(2), interval(3)));
    ctxCopy.put(Keys.TRUNCATED, true);
    Assert.assertEquals(
        ctxCopy.getDelegate(),
        deserializeContext(res2.getResult(), objectMapper)
    );
  }

  @Test
  public void deserializeTest() throws IOException
  {
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    final ResponseContext ctx = ResponseContext.deserialize(
        mapper.writeValueAsString(
            ImmutableMap.of(
                Keys.ETAG.getName(), "string-value",
                Keys.NUM_SCANNED_ROWS.getName(), 100L,
                Keys.CPU_CONSUMED_NANOS.getName(), 100000L
            )
        ),
        mapper
    );
    Assert.assertEquals("string-value", ctx.getEntityTag());
    Assert.assertEquals((Long) 100L, ctx.getRowScanCount());
    Assert.assertEquals((Long) 100000L, ctx.getCpuNanos());
    ctx.addRowScanCount(10L);
    Assert.assertEquals((Long) 110L, ctx.getRowScanCount());
    ctx.addCpuNanos(100L);
    Assert.assertEquals((Long) 100100L, ctx.getCpuNanos());
  }

  @Test
  public void extensionEnumMergeTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.putEntityTag("etag");
    ctx.add(EXTN_STRING_KEY, "string-value");
    ctx.add(EXTN_COUNTER_KEY, 2L);
    final ResponseContext ctxFinal = ResponseContext.createEmpty();
    ctxFinal.putEntityTag("old-etag");
    ctxFinal.add(EXTN_STRING_KEY, "old-string-value");
    ctxFinal.add(EXTN_COUNTER_KEY, 1L);
    ctxFinal.merge(ctx);
    Assert.assertEquals("etag", ctxFinal.getEntityTag());
    Assert.assertEquals("string-value", ctxFinal.get(EXTN_STRING_KEY));
    Assert.assertEquals(1L + 2L, ctxFinal.get(EXTN_COUNTER_KEY));
  }

  @Test
  public void toMapTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.putEntityTag("etag");
    Map<String, Object> map = ctx.toMap();
    Assert.assertEquals(map.get(ResponseContext.Keys.ETAG.getName()), "etag");
  }
}
