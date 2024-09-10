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
    final String[] strings = {"BBBB", "AAAAA", "CCCC", "DDDD", "EEEE"};
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

    Assert.assertEquals("AAAAA", result);
  }

  @Test
  public void testBufferAggregateWithFoldCheck()
  {
    final String[] strings = {"AAAA", "BBBB", "CCCC", "DDDD", "EEEE"};
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
    final String[] strings = {"CCCC", "AAAA", "BBBB", null, "EEEE"};
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
  public void testNullFirstBufferAggregate()
  {
    final String[] strings = {null, "CCCC", "AAAA", "BBBB", "EEEE"};
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
}
