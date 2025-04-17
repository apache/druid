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

public class StringMaxAggregatorTest
{
  @Test
  public void testAggregate()
  {
    final String[] strings = {"Maxstrings", "MaxStrings", "MaxString", "Maxstring", "AAAAAAAAAAAAAAAAAAAAAAAAA"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMaxAggregator agg = new StringMaxAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();
    agg.close();

    Assert.assertEquals("Maxstrings", result);
  }

  @Test
  public void testAggregateWithDuplicateValues()
  {
    final String[] strings = {"AAAA", "AAAA", "aaaa", "aaaa", "BBBB"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMaxAggregator agg = new StringMaxAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();
    agg.close();

    Assert.assertEquals("aaaa", result);
  }

  @Test
  public void testContainsNullAggregate()
  {
    final String[] strings = {null, "AAAA", "BBBB", null, "CCCC"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMaxAggregator agg = new StringMaxAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();
    agg.close();

    Assert.assertEquals("CCCC", result);
  }

  @Test
  public void testSpecialCharactersAggregate()
  {
    final String[] strings = {"ZZZZ", "@@@@", "!!!!", "####", "&&&&"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMaxAggregator agg = new StringMaxAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();
    agg.close();

    Assert.assertEquals("ZZZZ", result);
  }

  @Test
  public void testEmptyStringAggregate()
  {
    final String[] strings = {"ZZZZ", "", "AAAA", "CCCC", "DDDD"};
    int maxStringBytes = 1024;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMaxAggregator agg = new StringMaxAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();
    agg.close();

    Assert.assertEquals("ZZZZ", result);
  }

  @Test
  public void testLongStringTruncation()
  {
    final String[] strings = {"ZZZZ", "a very very long string that exceeds the max bytes", "AAAA", "CCCC", "DDDD"};
    int maxStringBytes = 10;

    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(strings);

    StringMaxAggregator agg = new StringMaxAggregator(objectColumnSelector, maxStringBytes);

    for (int i = 0; i < strings.length; i++) {
      agg.aggregate();
      objectColumnSelector.increment();
    }

    String result = (String) agg.get();
    agg.close();

    Assert.assertEquals("a very ver", result);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetFloat()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{"AAAA"});

    StringMaxAggregator agg = new StringMaxAggregator(objectColumnSelector, maxStringBytes);
    agg.getFloat();
    agg.close();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetLong()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{"AAAA"});

    StringMaxAggregator agg = new StringMaxAggregator(objectColumnSelector, maxStringBytes);
    agg.getLong();
    agg.close();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetDouble()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{"AAAA"});

    StringMaxAggregator agg = new StringMaxAggregator(objectColumnSelector, maxStringBytes);
    agg.getDouble();
    agg.close();
  }

  @Test
  public void testIsNull()
  {
    int maxStringBytes = 1024;
    TestObjectColumnSelector<String> objectColumnSelector = new TestObjectColumnSelector<>(new String[]{null, "AAAA"});

    StringMaxAggregator agg = new StringMaxAggregator(objectColumnSelector, maxStringBytes);

    Assert.assertTrue(agg.isNull());

    agg.aggregate();
    objectColumnSelector.increment();
    agg.aggregate();

    Assert.assertFalse(agg.isNull());
    agg.close();
  }

  @Test
  public void testCombineValues()
  {
    Assert.assertNull(StringMaxAggregator.combineValues(null, null));
    Assert.assertEquals("a", StringMaxAggregator.combineValues("a", null));
    Assert.assertEquals("a", StringMaxAggregator.combineValues(null, "a"));
    Assert.assertEquals("b", StringMaxAggregator.combineValues("a", "b"));
    Assert.assertEquals("b", StringMaxAggregator.combineValues("b", "a"));
    Assert.assertEquals("b", StringMaxAggregator.combineValues("b", "b"));
  }
}
