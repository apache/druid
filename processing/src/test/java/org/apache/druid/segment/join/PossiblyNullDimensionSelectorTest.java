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

package org.apache.druid.segment.join;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PossiblyNullDimensionSelectorTest
{
  private boolean isNull = false;

  private final DimensionSelector onNullSelector = makeSelector(DimensionSelector.constant(null));
  private final DimensionSelector onNonnullSelector = makeSelector(DimensionSelector.constant("foo"));

  @Test
  public void test_getRow_normalOnNullSelector()
  {
    isNull = false;
    assertRowsEqual(new int[]{0}, onNullSelector.getRow());
  }

  @Test
  public void test_getRow_nullOnNullSelector()
  {
    isNull = true;
    assertRowsEqual(new int[]{0}, onNullSelector.getRow());
  }

  @Test
  public void test_getRow_normalOnNonnullSelector()
  {
    isNull = false;
    assertRowsEqual(new int[]{1}, onNonnullSelector.getRow());
  }

  @Test
  public void test_getRow_nullOnNonnullSelector()
  {
    isNull = true;
    assertRowsEqual(new int[]{0}, onNonnullSelector.getRow());
  }

  @Test
  public void test_getValueCardinality_onNullSelector()
  {
    Assertions.assertEquals(1, onNullSelector.getValueCardinality());
  }

  @Test
  public void test_getValueCardinality_onNonnullSelector()
  {
    Assertions.assertEquals(2, onNonnullSelector.getValueCardinality());
  }

  @Test
  public void test_lookupName_onNullSelector()
  {
    Assertions.assertNull(onNullSelector.lookupName(0));
  }

  @Test
  public void test_lookupName_onNonnullSelector()
  {
    Assertions.assertNull(onNonnullSelector.lookupName(0));
    Assertions.assertEquals("foo", onNonnullSelector.lookupName(1));
  }

  @Test
  public void test_lookupId_onNullSelector()
  {
    Assertions.assertEquals(0, onNullSelector.idLookup().lookupId(null));
  }

  @Test
  public void test_lookupId_onNonnullSelector()
  {
    Assertions.assertEquals(0, onNonnullSelector.idLookup().lookupId(null));
    Assertions.assertEquals(1, onNonnullSelector.idLookup().lookupId("foo"));
  }

  @Test
  public void test_nameLookupPossibleInAdvance_onNullSelector()
  {
    Assertions.assertTrue(onNonnullSelector.nameLookupPossibleInAdvance());
  }

  @Test
  public void test_nameLookupPossibleInAdvance_onNonnullSelector()
  {
    Assertions.assertTrue(onNonnullSelector.nameLookupPossibleInAdvance());
  }

  private DimensionSelector makeSelector(final DimensionSelector baseSelector)
  {
    return new PossiblyNullDimensionSelector(baseSelector, () -> isNull);
  }

  private static void assertRowsEqual(final int[] expected, final IndexedInts actual)
  {
    Assertions.assertEquals(IntArrayList.wrap(expected), toList(actual));
  }

  private static IntList toList(final IndexedInts ints)
  {
    final IntList retVal = new IntArrayList(ints.size());

    final int size = ints.size();
    for (int i = 0; i < size; i++) {
      retVal.add(ints.get(i));
    }

    return retVal;
  }
}
