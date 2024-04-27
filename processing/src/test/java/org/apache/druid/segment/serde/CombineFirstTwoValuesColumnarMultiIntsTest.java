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

package org.apache.druid.segment.serde;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.VSizeColumnarInts;
import org.apache.druid.segment.data.VSizeColumnarMultiInts;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Test for {@link CombineFirstTwoValuesColumnarMultiInts}.
 */
public class CombineFirstTwoValuesColumnarMultiIntsTest
{
  private ColumnarMultiInts original;
  private CombineFirstTwoValuesColumnarMultiInts combined;

  @Before
  public void setUp()
  {
    original = VSizeColumnarMultiInts.fromIterable(
        ImmutableList.of(
            VSizeColumnarInts.fromArray(new int[]{1, 2, 3}),
            VSizeColumnarInts.fromArray(new int[]{0, 1, 2, 3}),
            VSizeColumnarInts.fromArray(new int[]{3, 0, 2, 1, 5, 0})
        )
    );

    combined = new CombineFirstTwoValuesColumnarMultiInts(original);
  }

  @Test
  public void testSize()
  {
    Assert.assertEquals(original.size(), combined.size());
  }

  @Test
  public void testGet()
  {
    assertEquals(new int[]{0, 1, 2}, combined.get(0));
    assertEquals(new int[]{0, 0, 1, 2}, combined.get(1));
    assertEquals(new int[]{2, 0, 1, 0, 4, 0}, combined.get(2));

    // "get" reuses a holder
    Assert.assertSame(combined.get(1), combined.get(0));
  }

  @Test
  public void testGetUnshared()
  {
    assertEquals(new int[]{0, 1, 2}, combined.getUnshared(0));
    assertEquals(new int[]{0, 0, 1, 2}, combined.getUnshared(1));
    assertEquals(new int[]{2, 0, 1, 0, 4, 0}, combined.getUnshared(2));

    // Unlike "get", "getUnshared" does not reuse a holder
    Assert.assertNotSame(combined.getUnshared(1), combined.getUnshared(0));
  }

  @Test
  public void testIndexOf()
  {
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> combined.indexOf(original.get(0))
    );
  }

  @Test
  public void testIsSorted()
  {
    Assert.assertFalse(combined.isSorted());
  }

  @Test
  public void testIterator()
  {
    final List<IndexedInts> fromIterator = Lists.newArrayList(combined.iterator());
    Assert.assertEquals(3, fromIterator.size());
    assertEquals(new int[]{0, 1, 2}, fromIterator.get(0));
    assertEquals(new int[]{0, 0, 1, 2}, fromIterator.get(1));
    assertEquals(new int[]{2, 0, 1, 0, 4, 0}, fromIterator.get(2));
  }

  public void assertEquals(final int[] expected, final IndexedInts actual)
  {
    final int sz = actual.size();
    final int[] actualArray = new int[sz];
    for (int i = 0; i < sz; i++) {
      actualArray[i] = actual.get(i);
    }

    Assert.assertArrayEquals(expected, actualArray);
  }
}
