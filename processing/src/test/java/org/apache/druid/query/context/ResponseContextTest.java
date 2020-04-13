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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class ResponseContextTest
{

  enum ExtensionResponseContextKey implements ResponseContext.BaseKey
  {
    EXTENSION_KEY_1("extension_key_1"),
    EXTENSION_KEY_2("extension_key_2", (oldValue, newValue) -> (long) oldValue + (long) newValue);

    static {
      for (ResponseContext.BaseKey key : values()) {
        ResponseContext.Key.registerKey(key);
      }
    }

    private final String name;
    private final BiFunction<Object, Object, Object> mergeFunction;

    ExtensionResponseContextKey(String name)
    {
      this.name = name;
      this.mergeFunction = (oldValue, newValue) -> newValue;
    }

    ExtensionResponseContextKey(String name, BiFunction<Object, Object, Object> mergeFunction)
    {
      this.name = name;
      this.mergeFunction = mergeFunction;
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public BiFunction<Object, Object, Object> getMergeFunction()
    {
      return mergeFunction;
    }
  }

  private final ResponseContext.BaseKey nonregisteredKey = new ResponseContext.BaseKey()
  {
    @Override
    public String getName()
    {
      return "non-registered-key";
    }

    @Override
    public BiFunction<Object, Object, Object> getMergeFunction()
    {
      return (Object a, Object b) -> a;
    }
  };

  @Test(expected = IllegalStateException.class)
  public void putISETest()
  {
    ResponseContext.createEmpty().put(nonregisteredKey, new Object());
  }

  @Test(expected = IllegalStateException.class)
  public void addISETest()
  {
    ResponseContext.createEmpty().add(nonregisteredKey, new Object());
  }

  @Test(expected = IllegalArgumentException.class)
  public void registerKeyIAETest()
  {
    ResponseContext.Key.registerKey(ResponseContext.Key.NUM_SCANNED_ROWS);
  }

  @Test
  public void mergeValueTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.add(ResponseContext.Key.ETAG, "dummy-etag");
    Assert.assertEquals("dummy-etag", ctx.get(ResponseContext.Key.ETAG));
    ctx.add(ResponseContext.Key.ETAG, "new-dummy-etag");
    Assert.assertEquals("new-dummy-etag", ctx.get(ResponseContext.Key.ETAG));

    final Interval interval01 = Intervals.of("2019-01-01/P1D");
    ctx.add(ResponseContext.Key.UNCOVERED_INTERVALS, Collections.singletonList(interval01));
    Assert.assertArrayEquals(
        Collections.singletonList(interval01).toArray(),
        ((List) ctx.get(ResponseContext.Key.UNCOVERED_INTERVALS)).toArray()
    );
    final Interval interval12 = Intervals.of("2019-01-02/P1D");
    final Interval interval23 = Intervals.of("2019-01-03/P1D");
    ctx.add(ResponseContext.Key.UNCOVERED_INTERVALS, Arrays.asList(interval12, interval23));
    Assert.assertArrayEquals(
        Arrays.asList(interval01, interval12, interval23).toArray(),
        ((List) ctx.get(ResponseContext.Key.UNCOVERED_INTERVALS)).toArray()
    );

    final SegmentDescriptor sd01 = new SegmentDescriptor(interval01, "01", 0);
    ctx.add(ResponseContext.Key.MISSING_SEGMENTS, Collections.singletonList(sd01));
    Assert.assertArrayEquals(
        Collections.singletonList(sd01).toArray(),
        ((List) ctx.get(ResponseContext.Key.MISSING_SEGMENTS)).toArray()
    );
    final SegmentDescriptor sd12 = new SegmentDescriptor(interval12, "12", 1);
    final SegmentDescriptor sd23 = new SegmentDescriptor(interval23, "23", 2);
    ctx.add(ResponseContext.Key.MISSING_SEGMENTS, Arrays.asList(sd12, sd23));
    Assert.assertArrayEquals(
        Arrays.asList(sd01, sd12, sd23).toArray(),
        ((List) ctx.get(ResponseContext.Key.MISSING_SEGMENTS)).toArray()
    );

    ctx.add(ResponseContext.Key.NUM_SCANNED_ROWS, 0L);
    Assert.assertEquals(0L, ctx.get(ResponseContext.Key.NUM_SCANNED_ROWS));
    ctx.add(ResponseContext.Key.NUM_SCANNED_ROWS, 1L);
    Assert.assertEquals(1L, ctx.get(ResponseContext.Key.NUM_SCANNED_ROWS));
    ctx.add(ResponseContext.Key.NUM_SCANNED_ROWS, 3L);
    Assert.assertEquals(4L, ctx.get(ResponseContext.Key.NUM_SCANNED_ROWS));

    ctx.add(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED, false);
    Assert.assertEquals(false, ctx.get(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED));
    ctx.add(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED, true);
    Assert.assertEquals(true, ctx.get(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED));
    ctx.add(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED, false);
    Assert.assertEquals(true, ctx.get(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED));
  }

  @Test
  public void mergeResponseContextTest()
  {
    final ResponseContext ctx1 = ResponseContext.createEmpty();
    ctx1.put(ResponseContext.Key.ETAG, "dummy-etag-1");
    final Interval interval01 = Intervals.of("2019-01-01/P1D");
    ctx1.put(ResponseContext.Key.UNCOVERED_INTERVALS, Collections.singletonList(interval01));
    ctx1.put(ResponseContext.Key.NUM_SCANNED_ROWS, 1L);

    final ResponseContext ctx2 = ResponseContext.createEmpty();
    ctx2.put(ResponseContext.Key.ETAG, "dummy-etag-2");
    final Interval interval12 = Intervals.of("2019-01-02/P1D");
    ctx2.put(ResponseContext.Key.UNCOVERED_INTERVALS, Collections.singletonList(interval12));
    final SegmentDescriptor sd01 = new SegmentDescriptor(interval01, "01", 0);
    ctx2.put(ResponseContext.Key.MISSING_SEGMENTS, Collections.singletonList(sd01));
    ctx2.put(ResponseContext.Key.NUM_SCANNED_ROWS, 2L);

    ctx1.merge(ctx2);
    Assert.assertEquals("dummy-etag-2", ctx1.get(ResponseContext.Key.ETAG));
    Assert.assertEquals(3L, ctx1.get(ResponseContext.Key.NUM_SCANNED_ROWS));
    Assert.assertArrayEquals(
        Arrays.asList(interval01, interval12).toArray(),
        ((List) ctx1.get(ResponseContext.Key.UNCOVERED_INTERVALS)).toArray()
    );
    Assert.assertArrayEquals(
        Collections.singletonList(sd01).toArray(),
        ((List) ctx1.get(ResponseContext.Key.MISSING_SEGMENTS)).toArray()
    );
  }

  @Test(expected = IllegalStateException.class)
  public void mergeISETest()
  {
    final ResponseContext ctx = new ResponseContext()
    {
      @Override
      protected Map<BaseKey, Object> getDelegate()
      {
        return ImmutableMap.of(nonregisteredKey, "non-registered-key");
      }
    };
    ResponseContext.createEmpty().merge(ctx);
  }

  @Test
  public void serializeWithCorrectnessTest() throws JsonProcessingException
  {
    final ResponseContext ctx1 = ResponseContext.createEmpty();
    ctx1.add(ResponseContext.Key.ETAG, "string-value");
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(
        mapper.writeValueAsString(ImmutableMap.of("ETag", "string-value")),
        ctx1.serializeWith(mapper, Integer.MAX_VALUE).getTruncatedResult()
    );

    final ResponseContext ctx2 = ResponseContext.createEmpty();
    ctx2.add(ResponseContext.Key.NUM_SCANNED_ROWS, 100);
    Assert.assertEquals(
        mapper.writeValueAsString(ImmutableMap.of("count", 100)),
        ctx2.serializeWith(mapper, Integer.MAX_VALUE).getTruncatedResult()
    );
  }

  @Test
  public void serializeWithTruncateValueTest() throws IOException
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.put(ResponseContext.Key.NUM_SCANNED_ROWS, 100);
    ctx.put(ResponseContext.Key.ETAG, "long-string-that-is-supposed-to-be-removed-from-result");
    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    final String fullString = objectMapper.writeValueAsString(ctx.getDelegate());
    final ResponseContext.SerializationResult res1 = ctx.serializeWith(objectMapper, Integer.MAX_VALUE);
    Assert.assertEquals(fullString, res1.getTruncatedResult());
    final ResponseContext ctxCopy = ResponseContext.createEmpty();
    ctxCopy.merge(ctx);
    final ResponseContext.SerializationResult res2 = ctx.serializeWith(objectMapper, 30);
    ctxCopy.remove(ResponseContext.Key.ETAG);
    ctxCopy.put(ResponseContext.Key.TRUNCATED, true);
    Assert.assertEquals(
        ctxCopy.getDelegate(),
        ResponseContext.deserialize(res2.getTruncatedResult(), objectMapper).getDelegate()
    );
  }

  @Test
  public void serializeWithTruncateArrayTest() throws IOException
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.put(ResponseContext.Key.NUM_SCANNED_ROWS, 100);
    ctx.put(
        ResponseContext.Key.UNCOVERED_INTERVALS,
        Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    );
    ctx.put(
        ResponseContext.Key.MISSING_SEGMENTS,
        Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    );
    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    final String fullString = objectMapper.writeValueAsString(ctx.getDelegate());
    final ResponseContext.SerializationResult res1 = ctx.serializeWith(objectMapper, Integer.MAX_VALUE);
    Assert.assertEquals(fullString, res1.getTruncatedResult());
    final ResponseContext ctxCopy = ResponseContext.createEmpty();
    ctxCopy.merge(ctx);
    final ResponseContext.SerializationResult res2 = ctx.serializeWith(objectMapper, 70);
    ctxCopy.put(ResponseContext.Key.UNCOVERED_INTERVALS, Arrays.asList(0, 1, 2, 3, 4));
    ctxCopy.remove(ResponseContext.Key.MISSING_SEGMENTS);
    ctxCopy.put(ResponseContext.Key.TRUNCATED, true);
    Assert.assertEquals(
        ctxCopy.getDelegate(),
        ResponseContext.deserialize(res2.getTruncatedResult(), objectMapper).getDelegate()
    );
  }

  @Test
  public void deserializeTest() throws IOException
  {
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    final ResponseContext ctx = ResponseContext.deserialize(
        mapper.writeValueAsString(
            ImmutableMap.of(
                "ETag", "string-value",
                "count", 100L,
                "cpuConsumed", 100000L
            )
        ),
        mapper
    );
    Assert.assertEquals("string-value", ctx.get(ResponseContext.Key.ETAG));
    Assert.assertEquals(100, ctx.get(ResponseContext.Key.NUM_SCANNED_ROWS));
    Assert.assertEquals(100000, ctx.get(ResponseContext.Key.CPU_CONSUMED_NANOS));
    ctx.add(ResponseContext.Key.NUM_SCANNED_ROWS, 10L);
    Assert.assertEquals(110L, ctx.get(ResponseContext.Key.NUM_SCANNED_ROWS));
    ctx.add(ResponseContext.Key.CPU_CONSUMED_NANOS, 100);
    Assert.assertEquals(100100L, ctx.get(ResponseContext.Key.CPU_CONSUMED_NANOS));
  }

  @Test(expected = IllegalStateException.class)
  public void deserializeISETest() throws IOException
  {
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    ResponseContext.deserialize(
        mapper.writeValueAsString(ImmutableMap.of("ETag_unexpected", "string-value")),
        mapper
    );
  }

  @Test
  public void extensionEnumIntegrityTest()
  {
    Assert.assertEquals(
        ExtensionResponseContextKey.EXTENSION_KEY_1,
        ResponseContext.Key.keyOf(ExtensionResponseContextKey.EXTENSION_KEY_1.getName())
    );
    Assert.assertEquals(
        ExtensionResponseContextKey.EXTENSION_KEY_2,
        ResponseContext.Key.keyOf(ExtensionResponseContextKey.EXTENSION_KEY_2.getName())
    );
    for (ResponseContext.BaseKey key : ExtensionResponseContextKey.values()) {
      Assert.assertTrue(ResponseContext.Key.getAllRegisteredKeys().contains(key));
    }
  }

  @Test
  public void extensionEnumMergeTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.add(ResponseContext.Key.ETAG, "etag");
    ctx.add(ExtensionResponseContextKey.EXTENSION_KEY_1, "string-value");
    ctx.add(ExtensionResponseContextKey.EXTENSION_KEY_2, 2L);
    final ResponseContext ctxFinal = ResponseContext.createEmpty();
    ctxFinal.add(ResponseContext.Key.ETAG, "old-etag");
    ctxFinal.add(ExtensionResponseContextKey.EXTENSION_KEY_1, "old-string-value");
    ctxFinal.add(ExtensionResponseContextKey.EXTENSION_KEY_2, 1L);
    ctxFinal.merge(ctx);
    Assert.assertEquals("etag", ctxFinal.get(ResponseContext.Key.ETAG));
    Assert.assertEquals("string-value", ctxFinal.get(ExtensionResponseContextKey.EXTENSION_KEY_1));
    Assert.assertEquals(1L + 2L, ctxFinal.get(ExtensionResponseContextKey.EXTENSION_KEY_2));
  }
}
