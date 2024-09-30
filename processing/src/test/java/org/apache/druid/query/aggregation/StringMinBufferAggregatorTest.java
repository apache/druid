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

package org.apache.druid.query.aggregation;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringMinBufferAggregatorTest
{
  private void aggregateBuffer(
      TestObjectColumnSelector<String> valueSelector,
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
    final String[] strings = {"Minstrings", "a", "MinStrings", "MinString", "Minstring"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinBufferAggregator agg = new StringMinBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer buf = ByteBuffer.allocate(2048);
    int position = 0;

    agg.init(buf, position);
    for (int i = 0; i < strings.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, position);
    }

    String result = (String) agg.get(buf, position);

    Assert.assertEquals("MinString", result);
  }

  @Test
  public void testBufferAggregateWithDuplicateValues()
  {
    final String[] strings = {"AAAA", "AAAA", "aaaa", "aaaa", "BBBB"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinBufferAggregator agg = new StringMinBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer buf = ByteBuffer.allocate(2048);
    int position = 0;

    agg.init(buf, position);
    for (int i = 0; i < strings.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, position);
    }

    String result = (String) agg.get(buf, position);

    Assert.assertEquals("AAAA", result);
  }

  @Test
  public void testContainsNullBufferAggregate()
  {
    final String[] strings = {null, "AAAA", "BBBB", null, "CCCC"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinBufferAggregator agg = new StringMinBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer buf = ByteBuffer.allocate(2048);
    int position = 0;

    agg.init(buf, position);
    for (int i = 0; i < strings.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, position);
    }

    String result = (String) agg.get(buf, position);

    Assert.assertEquals("AAAA", result);
  }

  @Test
  public void testSpecialCharactersBufferAggregate()
  {
    final String[] strings = {"ZZZZ", "@@@@", "!!!!", "####", "&&&&"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinBufferAggregator agg = new StringMinBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer buf = ByteBuffer.allocate(2048);
    int position = 0;

    agg.init(buf, position);
    for (int i = 0; i < strings.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, position);
    }

    String result = (String) agg.get(buf, position);

    Assert.assertEquals("!!!!", result);
  }

  @Test
  public void testDifferentBufferPositions()
  {
    final String[] strings = {"BBBB", "AAAA", "CCCC", "DDDD", "EEEE"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinBufferAggregator agg = new StringMinBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer buf = ByteBuffer.allocate(2048);
    int position1 = 0;
    int position2 = 1024;

    agg.init(buf, position1);
    agg.init(buf, position2);

    for (int i = 0; i < strings.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, buf, position1);
      aggregateBuffer(objectColumnSelector, agg, buf, position2);
    }

    String result1 = (String) agg.get(buf, position1);
    String result2 = (String) agg.get(buf, position2);

    Assert.assertEquals("AAAA", result1);
    Assert.assertEquals("AAAA", result2);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetFloat()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{"AAAA"});

    StringMinBufferAggregator agg = new StringMinBufferAggregator(objectColumnSelector, maxStringBytes);
    ByteBuffer buf = ByteBuffer.allocate(2048);
    int position = 0;

    agg.init(buf, position);
    agg.getFloat(buf, position);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetLong()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{"AAAA"});

    StringMinBufferAggregator agg = new StringMinBufferAggregator(objectColumnSelector, maxStringBytes);
    ByteBuffer buf = ByteBuffer.allocate(2048);
    int position = 0;

    agg.init(buf, position);
    agg.getLong(buf, position);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetDouble()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{"AAAA"});

    StringMinBufferAggregator agg = new StringMinBufferAggregator(objectColumnSelector, maxStringBytes);
    ByteBuffer buf = ByteBuffer.allocate(2048);
    int position = 0;

    agg.init(buf, position);
    agg.getDouble(buf, position);
  }

  @Test
  public void testRelocate()
  {
    final String[] strings = {"AAAA", "BBBB", "CCCC", "DDDD", "EEEE"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinBufferAggregator agg = new StringMinBufferAggregator(
        objectColumnSelector,
        maxStringBytes
    );

    ByteBuffer oldBuf = ByteBuffer.allocate(2048);
    ByteBuffer newBuf = ByteBuffer.allocate(2048);
    int oldPosition = 0;
    int newPosition = 1024;

    agg.init(oldBuf, oldPosition);
    for (int i = 0; i < strings.length; i++) {
      aggregateBuffer(objectColumnSelector, agg, oldBuf, oldPosition);
    }

    agg.relocate(oldPosition, newPosition, oldBuf, newBuf);

    String result = (String) agg.get(newBuf, newPosition);

    Assert.assertEquals("AAAA", result);
  }
}
