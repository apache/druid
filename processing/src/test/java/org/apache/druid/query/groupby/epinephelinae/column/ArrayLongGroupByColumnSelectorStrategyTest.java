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

package org.apache.druid.query.groupby.epinephelinae.column;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;

@RunWith(MockitoJUnitRunner.class)
public class ArrayLongGroupByColumnSelectorStrategyTest
{
  private final ByteBuffer buffer1 = ByteBuffer.allocate(4);
  private final ByteBuffer buffer2 = ByteBuffer.allocate(4);

  private ArrayNumericGroupByColumnSelectorStrategy strategy;

  @Before
  public void setup()
  {
    strategy = new ArrayLongGroupByColumnSelectorStrategy();
    addToStrategy(new Object[]{1L, 2L});
    addToStrategy(ImmutableList.of(2L, 3L));
    addToStrategy(new Long[]{1L});
  }

  @Test
  public void testKeySize()
  {
    Assert.assertEquals(Integer.BYTES, strategy.getGroupingKeySize());
  }

  @Test
  public void testWriteKey()
  {
    strategy.writeToKeyBuffer(0, 1, buffer1);
    Assert.assertEquals(1, buffer1.getInt(0));
  }

  @Test
  public void testBufferComparatorsWithNullAndNonNullStringComprators()
  {
    buffer1.putInt(1);
    buffer2.putInt(2);
    Grouper.BufferComparator comparator = strategy.bufferComparator(0, null);
    Assert.assertTrue(comparator.compare(buffer1, buffer2, 0, 0) > 0);
    Assert.assertTrue(comparator.compare(buffer2, buffer1, 0, 0) < 0);

    comparator = strategy.bufferComparator(0, StringComparators.LEXICOGRAPHIC);
    Assert.assertTrue(comparator.compare(buffer1, buffer2, 0, 0) > 0);
    Assert.assertTrue(comparator.compare(buffer2, buffer1, 0, 0) < 0);

    comparator = strategy.bufferComparator(0, StringComparators.STRLEN);
    Assert.assertTrue(comparator.compare(buffer1, buffer2, 0, 0) > 0);
    Assert.assertTrue(comparator.compare(buffer2, buffer1, 0, 0) < 0);
  }

  @Test
  public void testBufferComparator()
  {
    buffer1.putInt(0);
    buffer2.putInt(2);
    Grouper.BufferComparator comparator = strategy.bufferComparator(0, null);
    Assert.assertTrue(comparator.compare(buffer1, buffer2, 0, 0) > 0);

  }

  @Test
  public void testSanity()
  {
    testSanity(new Object[]{1L, 2L}, 0);
    testSanity(new Object[]{2L, 3L}, 1);
    testSanity(new Object[]{1L}, 2);
  }

  private void testSanity(Object[] storedValue, int expectedIndex)
  {
    ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
    Mockito.when(columnValueSelector.getObject()).thenReturn(storedValue);
    Assert.assertEquals(expectedIndex, strategy.computeDictionaryId(columnValueSelector));

    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    ResultRow row = ResultRow.create(1);

    buffer1.putInt(0, expectedIndex);
    strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, buffer1, row, 0);
    Assert.assertArrayEquals(storedValue, (Object[]) row.get(0));
  }

  @Test
  public void testAddingInDictionary()
  {
    ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
    Mockito.when(columnValueSelector.getObject()).thenReturn(ImmutableList.of(4L, 2L));
    Assert.assertEquals(3, strategy.computeDictionaryId(columnValueSelector));

    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    ResultRow row = ResultRow.create(1);

    buffer1.putInt(3);
    strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, buffer1, row, 0);
    Assert.assertArrayEquals(new Object[]{4L, 2L}, (Object[]) row.get(0));
  }

  @Test
  public void testAddingInDictionaryWithObjects()
  {
    ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
    Mockito.when(columnValueSelector.getObject()).thenReturn(new Object[]{4L, 2L});
    Assert.assertEquals(3, strategy.computeDictionaryId(columnValueSelector));

    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    ResultRow row = ResultRow.create(1);

    buffer1.putInt(3);
    strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, buffer1, row, 0);
    Assert.assertArrayEquals(new Object[]{4L, 2L}, (Object[]) row.get(0));
  }

  private void addToStrategy(Object value)
  {
    ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
    Mockito.when(columnValueSelector.getObject()).thenReturn(value);
    strategy.computeDictionaryId(columnValueSelector);
  }

  @After
  public void tearDown()
  {
    buffer1.clear();
    buffer2.clear();
  }
}
