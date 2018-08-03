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

package io.druid.query.aggregation.first;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.SerializablePairLongString;
import io.druid.query.aggregation.TestLongColumnSelector;
import io.druid.query.aggregation.TestObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringFirstBufferAggregatorTest
{
  private void aggregateBuffer(
      TestLongColumnSelector timeSelector,
      TestObjectColumnSelector valueSelector,
      BufferAggregator agg,
      ByteBuffer buf,
      int position
  )
  {
    agg.aggregate(buf, position);
    timeSelector.increment();
    valueSelector.increment();
  }

  @Test
  public void testBufferAggregate() throws Exception
  {

    final long[] timestamps = {1526724600L, 1526724700L, 1526724800L, 1526725900L, 1526725000L};
    final String[] strings = {"AAAA", "BBBB", "CCCC", "DDDD", "EEEE"};
    Integer maxStringBytes = 1024;

    TestLongColumnSelector longColumnSelector = new TestLongColumnSelector(timestamps);
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringFirstAggregatorFactory factory = new StringFirstAggregatorFactory(
        "billy", "billy", maxStringBytes
    );

    StringFirstBufferAggregator agg = new StringFirstBufferAggregator(
        longColumnSelector,
        objectColumnSelector,
        maxStringBytes
    );

    String testString = "ZZZZ";

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    buf.putLong(1526728500L);
    buf.putInt(testString.length());
    buf.put(testString.getBytes(StandardCharsets.UTF_8));

    int position = 0;

    agg.init(buf, position);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < timestamps.length; i++) {
      aggregateBuffer(longColumnSelector, objectColumnSelector, agg, buf, position);
    }

    SerializablePairLongString sp = ((SerializablePairLongString) agg.get(buf, position));


    Assert.assertEquals("expectec last string value", strings[0], sp.rhs);
    Assert.assertEquals("last string timestamp is the biggest", new Long(timestamps[0]), new Long(sp.lhs));

  }

  @Test
  public void testNullBufferAggregate() throws Exception
  {

    final long[] timestamps = {2222L, 1111L, 3333L, 4444L, 5555L};
    final String[] strings = {null, "AAAA", "BBBB", "DDDD", "EEEE"};
    Integer maxStringBytes = 1024;

    TestLongColumnSelector longColumnSelector = new TestLongColumnSelector(timestamps);
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringFirstAggregatorFactory factory = new StringFirstAggregatorFactory(
        "billy", "billy", maxStringBytes
    );

    StringFirstBufferAggregator agg = new StringFirstBufferAggregator(
        longColumnSelector,
        objectColumnSelector,
        maxStringBytes
    );

    String testString = "ZZZZ";

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    buf.putLong(1526728500L);
    buf.putInt(testString.length());
    buf.put(testString.getBytes(StandardCharsets.UTF_8));

    int position = 0;

    agg.init(buf, position);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < timestamps.length; i++) {
      aggregateBuffer(longColumnSelector, objectColumnSelector, agg, buf, position);
    }

    SerializablePairLongString sp = ((SerializablePairLongString) agg.get(buf, position));


    Assert.assertEquals("expectec last string value", strings[1], sp.rhs);
    Assert.assertEquals("last string timestamp is the biggest", new Long(timestamps[1]), new Long(sp.lhs));

  }

  @Test(expected = IllegalStateException.class)
  public void testNoStringValue()
  {

    final long[] timestamps = {1526724000L, 1526724600L};
    final Double[] doubles = {null, 2.00};
    Integer maxStringBytes = 1024;

    TestLongColumnSelector longColumnSelector = new TestLongColumnSelector(timestamps);
    TestObjectColumnSelector<Double> objectColumnSelector = new TestObjectColumnSelector<>(doubles);

    StringFirstAggregatorFactory factory = new StringFirstAggregatorFactory(
        "billy", "billy", maxStringBytes
    );

    StringFirstBufferAggregator agg = new StringFirstBufferAggregator(
        longColumnSelector,
        objectColumnSelector,
        maxStringBytes
    );

    String testString = "ZZZZ";

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    buf.putLong(1526728500L);
    buf.putInt(testString.length());
    buf.put(testString.getBytes(StandardCharsets.UTF_8));

    int position = 0;

    agg.init(buf, position);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < timestamps.length; i++) {
      aggregateBuffer(longColumnSelector, objectColumnSelector, agg, buf, position);
    }
  }
}
