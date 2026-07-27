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

package org.apache.druid.segment.join.table;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.apache.druid.segment.column.ColumnType;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RowBasedIndexBuilderTest
{
  @Test
  public void test_stringKey_uniqueKeys()
  {
    final RowBasedIndexBuilder builder =
        new RowBasedIndexBuilder(ColumnType.STRING)
            .add("abc")
            .add("")
            .add("1")
            .add("def");

    final IndexedTable.Index index = builder.build();

    MatcherAssert.assertThat(index, CoreMatchers.instanceOf(MapIndex.class));
    Assertions.assertEquals(ColumnType.STRING, index.keyType());
    Assertions.assertTrue(index.areKeysUnique(false));
    Assertions.assertTrue(index.areKeysUnique(true));

    Assertions.assertEquals(intSet(0), index.find("abc"));
    Assertions.assertEquals(intSet(1), index.find(""));
    Assertions.assertEquals(intSet(2), index.find(1L));
    Assertions.assertEquals(intSet(2), index.find("1"));
    Assertions.assertEquals(intSet(3), index.find("def"));
    Assertions.assertEquals(intSet(), index.find(null));
    Assertions.assertEquals(intSet(), index.find("nonexistent"));

    Assertions.assertThrows(UnsupportedOperationException.class, () -> index.findUniqueLong(0L));
  }

  @Test
  public void test_stringKey_uniqueKeysWithNull()
  {
    final RowBasedIndexBuilder builder =
        new RowBasedIndexBuilder(ColumnType.STRING)
            .add("abc")
            .add("")
            .add(null)
            .add("1")
            .add("def");

    final IndexedTable.Index index = builder.build();

    MatcherAssert.assertThat(index, CoreMatchers.instanceOf(MapIndex.class));
    Assertions.assertEquals(ColumnType.STRING, index.keyType());
    Assertions.assertTrue(index.areKeysUnique(false));
    Assertions.assertTrue(index.areKeysUnique(true));

    Assertions.assertEquals(intSet(0), index.find("abc"));
    Assertions.assertEquals(intSet(1), index.find(""));
    Assertions.assertEquals(intSet(3), index.find(1L));
    Assertions.assertEquals(intSet(3), index.find("1"));
    Assertions.assertEquals(intSet(4), index.find("def"));
    Assertions.assertEquals(intSet(2), index.find(null));
    Assertions.assertEquals(intSet(), index.find("nonexistent"));

    Assertions.assertThrows(UnsupportedOperationException.class, () -> index.findUniqueLong(0L));
  }

  @Test
  public void test_stringKey_duplicateNullKey()
  {
    final RowBasedIndexBuilder builder =
        new RowBasedIndexBuilder(ColumnType.STRING)
            .add("abc")
            .add("")
            .add(null)
            .add("1")
            .add(null)
            .add("def");

    final IndexedTable.Index index = builder.build();

    MatcherAssert.assertThat(index, CoreMatchers.instanceOf(MapIndex.class));
    Assertions.assertEquals(ColumnType.STRING, index.keyType());
    Assertions.assertTrue(index.areKeysUnique(false));
    Assertions.assertFalse(index.areKeysUnique(true));

    Assertions.assertEquals(intSet(0), index.find("abc"));
    Assertions.assertEquals(intSet(1), index.find(""));
    Assertions.assertEquals(intSet(3), index.find(1L));
    Assertions.assertEquals(intSet(3), index.find("1"));
    Assertions.assertEquals(intSet(5), index.find("def"));
    Assertions.assertEquals(intSet(2, 4), index.find(null));
    Assertions.assertEquals(intSet(), index.find("nonexistent"));

    Assertions.assertThrows(UnsupportedOperationException.class, () -> index.findUniqueLong(0L));
  }

  @Test
  public void test_stringKey_duplicateKeys()
  {
    final RowBasedIndexBuilder builder =
        new RowBasedIndexBuilder(ColumnType.STRING)
            .add("abc")
            .add("")
            .add(null)
            .add("abc")
            .add("1")
            .add("def");

    final IndexedTable.Index index = builder.build();

    MatcherAssert.assertThat(index, CoreMatchers.instanceOf(MapIndex.class));
    Assertions.assertEquals(ColumnType.STRING, index.keyType());
    Assertions.assertFalse(index.areKeysUnique(false));
    Assertions.assertFalse(index.areKeysUnique(true));

    Assertions.assertEquals(intSet(0, 3), index.find("abc"));
    Assertions.assertEquals(intSet(1), index.find(""));
    Assertions.assertEquals(intSet(4), index.find(1L));
    Assertions.assertEquals(intSet(4), index.find("1"));
    Assertions.assertEquals(intSet(5), index.find("def"));
    Assertions.assertEquals(intSet(2), index.find(null));
    Assertions.assertEquals(intSet(), index.find("nonexistent"));

    Assertions.assertThrows(UnsupportedOperationException.class, () -> index.findUniqueLong(0L));
  }

  @Test
  public void test_longKey_uniqueKeys()
  {
    final RowBasedIndexBuilder builder =
        new RowBasedIndexBuilder(ColumnType.LONG)
            .add(1)
            .add(5)
            .add(2);

    final IndexedTable.Index index = builder.build();

    MatcherAssert.assertThat(index, CoreMatchers.instanceOf(UniqueLongArrayIndex.class));
    Assertions.assertEquals(ColumnType.LONG, index.keyType());
    Assertions.assertTrue(index.areKeysUnique(false));

    Assertions.assertEquals(intSet(0), index.find(1L));
    Assertions.assertEquals(intSet(1), index.find(5L));
    Assertions.assertEquals(intSet(2), index.find(2L));
    Assertions.assertEquals(intSet(), index.find(3L));
    Assertions.assertEquals(intSet(), index.find(null));

    Assertions.assertEquals(0, index.findUniqueLong(1L));
    Assertions.assertEquals(1, index.findUniqueLong(5L));
    Assertions.assertEquals(2, index.findUniqueLong(2L));
    Assertions.assertEquals(IndexedTable.Index.NOT_FOUND, index.findUniqueLong(3L));
  }

  @Test
  public void test_longKey_uniqueKeysWithNull()
  {
    final RowBasedIndexBuilder builder =
        new RowBasedIndexBuilder(ColumnType.LONG)
            .add(1)
            .add(5)
            .add(2)
            .add(null);

    final IndexedTable.Index index = builder.build();

    MatcherAssert.assertThat(index, CoreMatchers.instanceOf(MapIndex.class));
    Assertions.assertEquals(ColumnType.LONG, index.keyType());
    Assertions.assertTrue(index.areKeysUnique(false));
    Assertions.assertTrue(index.areKeysUnique(true));

    Assertions.assertEquals(intSet(0), index.find(1L));
    Assertions.assertEquals(intSet(1), index.find(5L));
    Assertions.assertEquals(intSet(2), index.find(2L));
    Assertions.assertEquals(intSet(), index.find(3L));
    Assertions.assertEquals(intSet(3), index.find(null));

    Assertions.assertEquals(0, index.findUniqueLong(1L));
    Assertions.assertEquals(1, index.findUniqueLong(5L));
    Assertions.assertEquals(2, index.findUniqueLong(2L));
    Assertions.assertEquals(IndexedTable.Index.NOT_FOUND, index.findUniqueLong(3L));
  }

  @Test
  public void test_longKey_uniqueKeys_farApart()
  {
    final RowBasedIndexBuilder builder =
        new RowBasedIndexBuilder(ColumnType.LONG)
            .add(1)
            .add(10_000_000)
            .add(2);

    final IndexedTable.Index index = builder.build();

    MatcherAssert.assertThat(index, CoreMatchers.instanceOf(MapIndex.class));
    Assertions.assertEquals(ColumnType.LONG, index.keyType());
    Assertions.assertTrue(index.areKeysUnique(false));

    Assertions.assertEquals(intSet(0), index.find(1L));
    Assertions.assertEquals(intSet(1), index.find(10_000_000L));
    Assertions.assertEquals(intSet(2), index.find(2L));
    Assertions.assertEquals(intSet(), index.find(3L));
    Assertions.assertEquals(intSet(), index.find(null));

    Assertions.assertEquals(0, index.findUniqueLong(1L));
    Assertions.assertEquals(1, index.findUniqueLong(10_000_000L));
    Assertions.assertEquals(2, index.findUniqueLong(2L));
    Assertions.assertEquals(IndexedTable.Index.NOT_FOUND, index.findUniqueLong(3L));
  }

  @Test
  public void test_longKey_duplicateKeys()
  {
    final RowBasedIndexBuilder builder =
        new RowBasedIndexBuilder(ColumnType.LONG)
            .add(1)
            .add(5)
            .add(1)
            .add(2);

    final IndexedTable.Index index = builder.build();

    MatcherAssert.assertThat(index, CoreMatchers.instanceOf(MapIndex.class));
    Assertions.assertEquals(ColumnType.LONG, index.keyType());
    Assertions.assertFalse(index.areKeysUnique(false));

    Assertions.assertEquals(intSet(0, 2), index.find("1"));
    Assertions.assertEquals(intSet(0, 2), index.find(1));
    Assertions.assertEquals(intSet(0, 2), index.find(1L));
    Assertions.assertEquals(intSet(1), index.find(5L));
    Assertions.assertEquals(intSet(3), index.find(2L));
    Assertions.assertEquals(intSet(), index.find(3L));

    Assertions.assertThrows(UnsupportedOperationException.class, () -> index.findUniqueLong(5L));
  }

  public IntSortedSet intSet(final int... ints)
  {
    final IntAVLTreeSet retVal = new IntAVLTreeSet();
    for (int i : ints) {
      retVal.add(i);
    }
    return retVal;
  }
}
