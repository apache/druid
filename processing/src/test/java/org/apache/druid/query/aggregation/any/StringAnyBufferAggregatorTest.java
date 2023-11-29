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

import com.google.common.collect.Lists;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class StringAnyBufferAggregatorTest
{
  StringAnyAggregatorFactory factory = new StringAnyAggregatorFactory(
      "billy", "billy", 1024, true
  );

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
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes,
        true
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
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);


    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes,
        true
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
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes,
        true
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
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes, true
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
    int maxStringBytes = 1024;

    TestObjectColumnSelector<Double> objectColumnSelector = new TestObjectColumnSelector<>(doubles);

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes,
        true
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

  @Test
  public void testMvds()
  {
    List<String> mvd = Lists.newArrayList("AAAA", "AAAAB", "AAAC");
    final Object[] mvds = {null, "CCCC", mvd, "BBBB", "EEEE"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<Object> objectColumnSelector = new TestObjectColumnSelector<>(mvds);

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes, true
    );

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize() * 2);
    int position = 0;

    int[] positions = new int[]{0, 1, 43, 100, 189};
    Arrays.stream(positions).forEach(i -> agg.init(buf, i));

    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < mvds.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, positions[i]);
    }
    String result = ((String) agg.get(buf, position));
    Assert.assertNull(result);

    for (int i = 0; i < positions.length; i++) {
      if (i == 2) {
        Assert.assertEquals(mvd.toString(), agg.get(buf, positions[2]));
      } else {
        Assert.assertEquals(mvds[i], agg.get(buf, positions[i]));
      }
    }
  }

  @Test
  public void testMvdsWithCustomAggregate()
  {
    List<String> mvd = Lists.newArrayList("AAAA", "AAAAB", "AAAC");
    final Object[] mvds = {null, "CCCC", mvd, "BBBB", "EEEE"};
    final int maxStringBytes = 1024;

    TestObjectColumnSelector<Object> objectColumnSelector = new TestObjectColumnSelector<>(mvds);

    StringAnyBufferAggregator agg = new StringAnyBufferAggregator(
        objectColumnSelector,
        maxStringBytes, false
    );

    ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize() * 2);
    int position = 0;

    int[] positions = new int[]{0, 1, 43, 100, 189};
    Arrays.stream(positions).forEach(i -> agg.init(buf, i));

    //noinspection ForLoopReplaceableByForEach
    for (int i = 0; i < mvds.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, positions[i]);
    }
    String result = ((String) agg.get(buf, position));
    Assert.assertNull(result);

    for (int i = 0; i < positions.length; i++) {
      if (i == 2) {
        // takes first in case of mvds
        Assert.assertEquals(mvd.get(0), agg.get(buf, positions[2]));
      } else {
        Assert.assertEquals(mvds[i], agg.get(buf, positions[i]));
      }
    }
  }
}
