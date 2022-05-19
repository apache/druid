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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class StringAnyBufferAggregatorTest
{
  private void aggregateBuffer(
      TestObjectColumnSelector valueSelector,
      BufferAggregator agg,
      ByteBuffer buf,
      int position
  )
  {
    agg.aggregate(buf, position);
    valueSelector.increment();
  }

  @Test
  public void testBufferAggregate()
  {

    final String[] strings = {"AAAA", "BBBB", "CCCC", "DDDD", "EEEE"};
    Integer maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringAnyAggregatorFactory factory = new StringAnyAggregatorFactory(
        "billy", "billy", maxStringBytes
    );

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    int position = 0;

    agg.init(buf, position);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < strings.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, position);
    }

    String result = ((String) agg.get(buf, position));

    Assert.assertEquals(strings[0], result);
  }

  @Test
  public void testBufferAggregateWithFoldCheck()
  {
    final String[] strings = {"AAAA", "BBBB", "CCCC", "DDDD", "EEEE"};
    Integer maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringAnyAggregatorFactory factory = new StringAnyAggregatorFactory(
        "billy", "billy", maxStringBytes
    );

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    int position = 0;

    agg.init(buf, position);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < strings.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, position);
    }

    String result = ((String) agg.get(buf, position));


    Assert.assertEquals(strings[0], result);
  }

  @Test
  public void testContainsNullBufferAggregate()
  {

    final String[] strings = {"CCCC", "AAAA", "BBBB", null, "EEEE"};
    Integer maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringAnyAggregatorFactory factory = new StringAnyAggregatorFactory(
        "billy", "billy", maxStringBytes
    );

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    int position = 0;

    agg.init(buf, position);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < strings.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, position);
    }

    String result = ((String) agg.get(buf, position));

    Assert.assertEquals(strings[0], result);
  }

  @Test
  public void testNullFirstBufferAggregate()
  {

    final String[] strings = {null, "CCCC", "AAAA", "BBBB", "EEEE"};
    Integer maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringAnyAggregatorFactory factory = new StringAnyAggregatorFactory(
        "billy", "billy", maxStringBytes
    );

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    int position = 0;

    agg.init(buf, position);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < strings.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, position);
    }

    String result = ((String) agg.get(buf, position));

    Assert.assertNull(result);
  }

  @Test
  public void testNonStringValue()
  {

    final Double[] doubles = {1.00, 2.00};
    Integer maxStringBytes = 1024;

    TestObjectColumnSelector<Double> objectColumnSelector = new TestObjectColumnSelector<>(doubles);

    StringAnyAggregatorFactory factory = new StringAnyAggregatorFactory(
        "billy", "billy", maxStringBytes
    );

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
    int position = 0;

    agg.init(buf, position);
    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < doubles.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, position);
    }

    String result = ((String) agg.get(buf, position));

    Assert.assertEquals("1.0", result);
  }
}
