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

public class StringMinAggregatorTest
{
  @Test
  public void testAggregate()
  {
    final String[] strings = {"Minstrings", "a", "MinStrings", "MinString", "Minstring"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinAggregator agg = new StringMinAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();

    Assert.assertEquals("MinString", result);
  }

  @Test
  public void testAggregateWithDuplicateValues()
  {
    final String[] strings = {"AAAA", "AAAA", "aaaa", "aaaa", "BBBB"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinAggregator agg = new StringMinAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();

    Assert.assertEquals("AAAA", result);
  }

  @Test
  public void testContainsNullAggregate()
  {
    final String[] strings = {null, "AAAA", "BBBB", null, "CCCC"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinAggregator agg = new StringMinAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();

    Assert.assertEquals("AAAA", result);
  }

  @Test
  public void testSpecialCharactersAggregate()
  {
    final String[] strings = {"ZZZZ", "@@@@", "!!!!", "####", "&&&&"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinAggregator agg = new StringMinAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();

    Assert.assertEquals("!!!!", result);
  }

  @Test
  public void testEmptyStringAggregate()
  {
    final String[] strings = {"ZZZZ", "", "AAAA", "CCCC", "DDDD"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinAggregator agg = new StringMinAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();

    Assert.assertEquals("", result);
  }

  @Test
  public void testLongStringTruncation()
  {
    final String[] strings = {"ZZZZ", "A very very long string that exceeds the max bytes", "AAAA", "CCCC", "DDDD"};
    int maxStringBytes = 10;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMinAggregator agg = new StringMinAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();

    Assert.assertEquals("A very ver", result);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetFloat()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{"AAAA"});

    StringMinAggregator agg = new StringMinAggregator(objectColumnSelector, maxStringBytes);
    agg.getFloat();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetLong()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{"AAAA"});

    StringMinAggregator agg = new StringMinAggregator(objectColumnSelector, maxStringBytes);
    agg.getLong();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetDouble()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{"AAAA"});

    StringMinAggregator agg = new StringMinAggregator(objectColumnSelector, maxStringBytes);
    agg.getDouble();
  }

  @Test
  public void testIsNull()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{null, "AAAA"});

    StringMinAggregator agg = new StringMinAggregator(objectColumnSelector, maxStringBytes);

    Assert.assertTrue(agg.isNull());

    agg.aggregate();
    objectColumnSelector.increment();
    agg.aggregate();

    Assert.assertFalse(agg.isNull());
  }

  @Test
  public void testCombineValues()
  {
    Assert.assertNull(StringMinAggregator.combineValues(null, null));
    Assert.assertEquals("a", StringMinAggregator.combineValues("a", null));
    Assert.assertEquals("a", StringMinAggregator.combineValues(null, "a"));
    Assert.assertEquals("a", StringMinAggregator.combineValues("a", "b"));
    Assert.assertEquals("a", StringMinAggregator.combineValues("b", "a"));
  }
}
