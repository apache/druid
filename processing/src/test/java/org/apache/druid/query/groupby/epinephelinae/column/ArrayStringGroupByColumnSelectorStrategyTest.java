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

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.data.ComparableIntArray;
import org.apache.druid.segment.data.ComparableStringArray;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.HashMap;

@RunWith(MockitoJUnitRunner.class)
public class ArrayStringGroupByColumnSelectorStrategyTest
{

  private final BiMap<String, Integer> DICTIONARY_INT = HashBiMap.create(new HashMap<String, Integer>()
  {
    {
      put("a", 0);
      put("b", 1);
      put("bd", 2);
      put("d", 3);
      put("e", 4);
    }
  });

  // The dictionary has been constructed such that the values are not sorted lexicographically
  // so we can tell when the comparator uses a lexicographic comparison and when it uses the indexes.
  private final BiMap<ComparableIntArray, Integer> INDEXED_INTARRAYS = HashBiMap.create(
      new HashMap<ComparableIntArray, Integer>()
      {
        {
          put(ComparableIntArray.of(0, 1), 0);
          put(ComparableIntArray.of(2, 4), 1);
          put(ComparableIntArray.of(0, 2), 2);
        }
      }
  );


  private final ByteBuffer buffer1 = ByteBuffer.allocate(4);
  private final ByteBuffer buffer2 = ByteBuffer.allocate(4);

  private ArrayStringGroupByColumnSelectorStrategy strategy;

  @Before
  public void setup()
  {
    strategy = new ArrayStringGroupByColumnSelectorStrategy(DICTIONARY_INT, INDEXED_INTARRAYS);
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
  public void testBufferComparatorCanCompareIntsAndNullStringComparatorShouldUseLexicographicComparator()
  {
    buffer1.putInt(1);
    buffer2.putInt(2);
    Grouper.BufferComparator comparator = strategy.bufferComparator(0, null);
    Assert.assertTrue(comparator.compare(buffer1, buffer2, 0, 0) > 0);
    Assert.assertTrue(comparator.compare(buffer2, buffer1, 0, 0) < 0);
  }

  @Test
  public void testBufferComparatorCanCompareIntsAndLexicographicStringComparatorShouldUseLexicographicComparator()
  {
    buffer1.putInt(1);
    buffer2.putInt(2);
    Grouper.BufferComparator comparator = strategy.bufferComparator(0, StringComparators.LEXICOGRAPHIC);
    Assert.assertTrue(comparator.compare(buffer1, buffer2, 0, 0) > 0);
    Assert.assertTrue(comparator.compare(buffer2, buffer1, 0, 0) < 0);
  }

  @Test
  public void testBufferComparatorCanCompareIntsAndStrLenStringComparatorShouldUseLexicographicComparator()
  {
    buffer1.putInt(1);
    buffer2.putInt(2);
    Grouper.BufferComparator comparator = strategy.bufferComparator(0, StringComparators.STRLEN);
    Assert.assertTrue(comparator.compare(buffer1, buffer2, 0, 0) > 0);
    Assert.assertTrue(comparator.compare(buffer2, buffer1, 0, 0) < 0);
  }

  @Test
  public void testSanity()
  {
    ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
    Mockito.when(columnValueSelector.getObject()).thenReturn(ImmutableList.of("a", "b"));
    Assert.assertEquals(0, strategy.computeDictionaryId(columnValueSelector));

    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    ResultRow row = ResultRow.create(1);

    buffer1.putInt(0);
    strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, buffer1, row, 0);
    Assert.assertEquals(ComparableStringArray.of("a", "b"), row.get(0));
  }


  @Test
  public void testAddingInDictionary()
  {
    ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
    Mockito.when(columnValueSelector.getObject()).thenReturn(ImmutableList.of("f", "a"));
    Assert.assertEquals(3, strategy.computeDictionaryId(columnValueSelector));

    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    ResultRow row = ResultRow.create(1);

    buffer1.putInt(3);
    strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, buffer1, row, 0);
    Assert.assertEquals(ComparableStringArray.of("f", "a"), row.get(0));
  }

  @Test
  public void testAddingInDictionaryWithObjects()
  {
    ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
    Mockito.when(columnValueSelector.getObject()).thenReturn(new Object[]{"f", "a"});
    Assert.assertEquals(3, strategy.computeDictionaryId(columnValueSelector));

    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    ResultRow row = ResultRow.create(1);

    buffer1.putInt(3);
    strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, buffer1, row, 0);
    Assert.assertEquals(ComparableStringArray.of("f", "a"), row.get(0));
  }

  @After
  public void tearDown()
  {
    buffer1.clear();
    buffer2.clear();
  }
}
