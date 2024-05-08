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

package org.apache.druid.frame.key;

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RowKeyComparatorTest extends InitializedNullHandlingTest
{
  static {
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());
  }

  static final RowSignature SIGNATURE =
      RowSignature.builder()
                  .add("1", ColumnType.LONG)
                  .add("2", ColumnType.STRING)
                  .add("3", ColumnType.LONG)
                  .add("4", ColumnType.DOUBLE)
                  .add("5", HyperUniquesAggregatorFactory.TYPE)
                  .add("6", HyperUniquesAggregatorFactory.TYPE)
                  .build();

  private static final Object[] OBJECTS1 = new Object[]{
      -1L,
      "foo",
      2L,
      -1.2,
      ByteRowKeyComparatorTest.makeHllCollector(5),
      ByteRowKeyComparatorTest.makeHllCollector(1)
  };
  private static final Object[] OBJECTS2 = new Object[]{
      -1L,
      null,
      2L,
      1.2d,
      ByteRowKeyComparatorTest.makeHllCollector(50),
      ByteRowKeyComparatorTest.makeHllCollector(5)
  };
  private static final Object[] OBJECTS3 = new Object[]{
      -1L,
      "bar",
      2L,
      1.2d,
      ByteRowKeyComparatorTest.makeHllCollector(5),
      ByteRowKeyComparatorTest.makeHllCollector(50)
  };
  private static final Object[] OBJECTS4 = new Object[]{
      -1L,
      "foo",
      2L,
      1.2d,
      ByteRowKeyComparatorTest.makeHllCollector(1),
      ByteRowKeyComparatorTest.makeHllCollector(5)
  };
  private static final Object[] OBJECTS5 = new Object[]{
      -1L,
      "foo",
      3L,
      1.2d,
      ByteRowKeyComparatorTest.makeHllCollector(50),
      ByteRowKeyComparatorTest.makeHllCollector(5)
  };
  private static final Object[] OBJECTS6 = new Object[]{
      -1L,
      "foo",
      2L,
      1.3d,
      ByteRowKeyComparatorTest.makeHllCollector(100),
      ByteRowKeyComparatorTest.makeHllCollector(20)
  };
  private static final Object[] OBJECTS7 = new Object[]{
      1L,
      "foo",
      2L,
      -1.2d,
      ByteRowKeyComparatorTest.makeHllCollector(5),
      ByteRowKeyComparatorTest.makeHllCollector(5)
  };
  private static final Object[] OBJECTS8 = new Object[]{
      1L,
      "foo",
      2L,
      -1.2d,
      ByteRowKeyComparatorTest.makeHllCollector(500),
      ByteRowKeyComparatorTest.makeHllCollector(100)
  };
  private static final Object[] OBJECTS9 = new Object[]{
      1L,
      "foo",
      2L,
      -1.2d,
      ByteRowKeyComparatorTest.makeHllCollector(500),
      ByteRowKeyComparatorTest.makeHllCollector(10)
  };

  static final List<Object[]> ALL_KEY_OBJECTS = Arrays.asList(
      OBJECTS1,
      OBJECTS2,
      OBJECTS3,
      OBJECTS4,
      OBJECTS5,
      OBJECTS6,
      OBJECTS7,
      OBJECTS8,
      OBJECTS9
  );

  @Test
  public void test_compare_DDDDDD() // DDDDDD = all descending
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.DESCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.DESCENDING),
        new KeyColumn("5", KeyOrder.DESCENDING),
        new KeyColumn("6", KeyOrder.DESCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS),
        sortUsingKeyComparator(keyColumns, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_AAAAAA() // AAAAAA = all ascending
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.ASCENDING),
        new KeyColumn("6", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS),
        sortUsingKeyComparator(keyColumns, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_ADDADD()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.DESCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.DESCENDING),
        new KeyColumn("6", KeyOrder.DESCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS),
        sortUsingKeyComparator(keyColumns, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_DAADAA()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.DESCENDING),
        new KeyColumn("5", KeyOrder.ASCENDING),
        new KeyColumn("6", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS),
        sortUsingKeyComparator(keyColumns, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_DADADA()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.DESCENDING),
        new KeyColumn("6", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS),
        sortUsingKeyComparator(keyColumns, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(RowKeyComparator.class)
                  .withNonnullFields("byteRowKeyComparatorDelegate")
                  .usingGetClass()
                  .verify();
  }

  private List<RowKey> sortUsingKeyComparator(final List<KeyColumn> keyColumns, final List<Object[]> objectss)
  {
    final List<RowKey> sortedKeys = new ArrayList<>();

    for (final Object[] objects : objectss) {
      sortedKeys.add(KeyTestUtils.createKey(SIGNATURE, objects));
    }

    sortedKeys.sort(RowKeyComparator.create(keyColumns, SIGNATURE));
    return sortedKeys;
  }

  private List<RowKey> sortUsingObjectComparator(final List<KeyColumn> keyColumns, final List<Object[]> objectss)
  {
    final List<Object[]> sortedObjectssCopy = objectss.stream().sorted(
        (o1, o2) -> {
          for (int i = 0; i < keyColumns.size(); i++) {
            final KeyColumn keyColumn = keyColumns.get(i);

            //noinspection unchecked, rawtypes
            final int cmp = Comparators.<Comparable>naturalNullsFirst()
                                       .compare((Comparable) o1[i], (Comparable) o2[i]);
            if (cmp != 0) {
              return keyColumn.order() == KeyOrder.DESCENDING ? -cmp : cmp;
            }
          }

          return 0;
        }
    ).collect(Collectors.toList());

    final List<RowKey> sortedKeys = new ArrayList<>();

    for (final Object[] objects : sortedObjectssCopy) {
      sortedKeys.add(KeyTestUtils.createKey(SIGNATURE, objects));
    }

    return sortedKeys;
  }
}
