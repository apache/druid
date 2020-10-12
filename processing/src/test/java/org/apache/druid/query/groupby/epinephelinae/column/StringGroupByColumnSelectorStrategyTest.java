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

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

@RunWith(MockitoJUnitRunner.class)
public class StringGroupByColumnSelectorStrategyTest
{
  // The dictionary has been constructed such that the values are not sorted lexicographically
  // so we can tell when the comparator uses a lexicographic comparison and when it uses the indexes.
  private static final Int2ObjectMap<String> DICTIONARY = new Int2ObjectArrayMap<>(
      new int[] {0, 1, 2},
      new String[] {"A", "F1", "D"}
  );

  private final ByteBuffer lhsBuffer = ByteBuffer.allocate(4);
  private final ByteBuffer rhsBuffer = ByteBuffer.allocate(4);

  @Mock
  private ColumnCapabilities capabilities;
  private final IntFunction<String> dictionaryLookup = DICTIONARY::get;

  private StringGroupByColumnSelectorStrategy target;

  @Before
  public void setUp()
  {
    lhsBuffer.putInt(1);
    rhsBuffer.putInt(2);
    Mockito.doReturn(true).when(capabilities).hasBitmapIndexes();
    Mockito.doReturn(ColumnCapabilities.Capable.TRUE).when(capabilities).areDictionaryValuesSorted();
    Mockito.doReturn(ColumnCapabilities.Capable.TRUE).when(capabilities).areDictionaryValuesUnique();
    target = new StringGroupByColumnSelectorStrategy(dictionaryLookup, capabilities);
  }

  @Test
  public void testBufferComparatorCannotCompareIntsAndNullStringComparatorShouldUseLexicographicComparator()
  {
    Mockito.when(capabilities.areDictionaryValuesSorted()).thenReturn(ColumnCapabilities.Capable.FALSE);
    // The comparator is not using the short circuit so it isn't comparing indexes.
    Grouper.BufferComparator comparator = target.bufferComparator(0, null);
    Assert.assertTrue(comparator.compare(lhsBuffer, rhsBuffer, 0, 0) > 0);
    Assert.assertTrue(comparator.compare(rhsBuffer, lhsBuffer, 0, 0) < 0);
  }

  @Test
  public void testBufferComparatorCanCompareIntsAndNullStringComparatorShouldUseLexicographicComparator()
  {
    Grouper.BufferComparator comparator = target.bufferComparator(0, null);
    Assert.assertTrue(comparator.compare(lhsBuffer, rhsBuffer, 0, 0) < 0);
    Assert.assertTrue(comparator.compare(rhsBuffer, lhsBuffer, 0, 0) > 0);
  }

  @Test
  public void testBufferComparatorCanCompareIntsAndLexicographicStringComparatorShouldUseLexicographicComparator()
  {
    Grouper.BufferComparator comparator = target.bufferComparator(0, StringComparators.LEXICOGRAPHIC);
    Assert.assertTrue(comparator.compare(lhsBuffer, rhsBuffer, 0, 0) < 0);
    Assert.assertTrue(comparator.compare(rhsBuffer, lhsBuffer, 0, 0) > 0);
  }

  @Test
  public void testBufferComparatorCanCompareIntsAndStrLenStringComparatorShouldUseLexicographicComparator()
  {
    Grouper.BufferComparator comparator = target.bufferComparator(0, StringComparators.STRLEN);
    Assert.assertTrue(comparator.compare(lhsBuffer, rhsBuffer, 0, 0) > 0);
    Assert.assertTrue(comparator.compare(rhsBuffer, lhsBuffer, 0, 0) < 0);
  }

  @After
  public void tearDown()
  {
    lhsBuffer.clear();
    rhsBuffer.clear();
  }
}
