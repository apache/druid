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
import com.google.common.hash.Hashing;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.StringUtils;
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

public class ByteRowKeyComparatorTest extends InitializedNullHandlingTest
{

  static {
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());
  }

  static final RowSignature NO_COMPLEX_SIGNATURE =
      RowSignature.builder()
                  .add("1", ColumnType.LONG)
                  .add("2", ColumnType.STRING)
                  .add("3", ColumnType.LONG)
                  .add("4", ColumnType.DOUBLE)
                  .build();

  static final RowSignature SIGNATURE =
      RowSignature.builder()
                  .add("1", HyperUniquesAggregatorFactory.TYPE)
                  .add("2", ColumnType.LONG)
                  .add("3", ColumnType.STRING)
                  .add("4", HyperUniquesAggregatorFactory.TYPE)
                  .add("5", ColumnType.LONG)
                  .add("6", ColumnType.DOUBLE)
                  .add("7", HyperUniquesAggregatorFactory.TYPE)
                  .add("8", HyperUniquesAggregatorFactory.TYPE)
                  .build();

  private static final Object[] OBJECTS1_WITHOUT_COMPLEX_COLUMN =
      new Object[]{-1L, "foo", 2L, -1.2};
  private static final Object[] OBJECTS2_WITHOUT_COMPLEX_COLUMN =
      new Object[]{-1L, null, 2L, 1.2d};
  private static final Object[] OBJECTS3_WITHOUT_COMPLEX_COLUMN =
      new Object[]{-1L, "bar", 2L, 1.2d};
  private static final Object[] OBJECTS4_WITHOUT_COMPLEX_COLUMN =
      new Object[]{-1L, "foo", 2L, 1.2d};
  private static final Object[] OBJECTS5_WITHOUT_COMPLEX_COLUMN =
      new Object[]{-1L, "foo", 3L, 1.2d};
  private static final Object[] OBJECTS6_WITHOUT_COMPLEX_COLUMN =
      new Object[]{-1L, "foo", 2L, 1.3d};
  private static final Object[] OBJECTS7_WITHOUT_COMPLEX_COLUMN =
      new Object[]{1L, "foo", 2L, -1.2d};
  private static final Object[] OBJECTS8_WITHOUT_COMPLEX_COLUMN =
      new Object[]{1L, "foo", 2L, -1.2d};
  private static final Object[] OBJECTS9_WITHOUT_COMPLEX_COLUMN =
      new Object[]{1L, "foo", 2L, -1.2d};

  private static final Object[] OBJECTS1 =
      new Object[]{
          null,
          -1L,
          "foo",
          makeHllCollector(5),
          2L,
          -1.2,
          makeHllCollector(5),
          makeHllCollector(1)
      };
  private static final Object[] OBJECTS2 =
      new Object[]{
          null,
          -1L,
          null,
          null,
          2L,
          1.2d,
          makeHllCollector(50),
          makeHllCollector(5)
      };
  private static final Object[] OBJECTS3 =
      new Object[]{
          makeHllCollector(50),
          -1L,
          "bar",
          makeHllCollector(5),
          2L,
          1.2d,
          makeHllCollector(5),
          makeHllCollector(50)
      };
  private static final Object[] OBJECTS4 =
      new Object[]{
          makeHllCollector(50),
          -1L,
          "foo",
          makeHllCollector(100),
          2L,
          1.2d,
          makeHllCollector(1),
          makeHllCollector(5)
      };
  private static final Object[] OBJECTS5 =
      new Object[]{
          makeHllCollector(1),
          -1L,
          "foo",
          makeHllCollector(5),
          3L,
          1.2d,
          null,
          makeHllCollector(5)
      };
  private static final Object[] OBJECTS6 =
      new Object[]{
          makeHllCollector(5),
          -1L,
          "foo",
          makeHllCollector(100),
          2L,
          1.3d,
          makeHllCollector(100),
          makeHllCollector(20)
      };
  private static final Object[] OBJECTS7 =
      new Object[]{
          makeHllCollector(100),
          1L,
          "foo",
          makeHllCollector(5),
          2L,
          -1.2d,
          null,
          null
      };
  private static final Object[] OBJECTS8 =
      new Object[]{
          makeHllCollector(5),
          1L,
          "foo",
          makeHllCollector(50),
          2L,
          -1.2d,
          makeHllCollector(500),
          makeHllCollector(100)
      };
  private static final Object[] OBJECTS9 =
      new Object[]{
          makeHllCollector(5),
          1L,
          "foo",
          makeHllCollector(50),
          2L,
          -1.2d,
          makeHllCollector(500),
          makeHllCollector(10)
      };

  static final List<Object[]> KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN = Arrays.asList(
      OBJECTS1_WITHOUT_COMPLEX_COLUMN,
      OBJECTS2_WITHOUT_COMPLEX_COLUMN,
      OBJECTS3_WITHOUT_COMPLEX_COLUMN,
      OBJECTS4_WITHOUT_COMPLEX_COLUMN,
      OBJECTS5_WITHOUT_COMPLEX_COLUMN,
      OBJECTS6_WITHOUT_COMPLEX_COLUMN,
      OBJECTS7_WITHOUT_COMPLEX_COLUMN,
      OBJECTS8_WITHOUT_COMPLEX_COLUMN,
      OBJECTS9_WITHOUT_COMPLEX_COLUMN
  );

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
  public void test_compare_AAAA_without_complex_column() // AAAA = all ascending, no complex column
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.DESCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.DESCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE),
        sortUsingByteKeyComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE)
    );
  }

  @Test
  public void test_compare_DDDD_without_complex_column() // DDDD = all descending, no complex columns
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE),
        sortUsingByteKeyComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE)
    );
  }

  @Test
  public void test_compare_DAAD_without_complex_column()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.DESCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE),
        sortUsingByteKeyComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE)
    );
  }

  @Test
  public void test_compare_ADDA_without_complex_column()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.DESCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE),
        sortUsingByteKeyComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE)
    );
  }

  @Test
  public void test_compare_DADA_without_complex_column()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE),
        sortUsingByteKeyComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE)
    );
  }

  @Test
  public void test_compare_DDDDDDDD() // DDDDDDDD = all descending
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.DESCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.DESCENDING),
        new KeyColumn("5", KeyOrder.DESCENDING),
        new KeyColumn("6", KeyOrder.DESCENDING),
        new KeyColumn("7", KeyOrder.DESCENDING),
        new KeyColumn("8", KeyOrder.DESCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE),
        sortUsingByteKeyComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE)
    );
  }

  @Test
  public void test_compare_AAAAAAAA() // AAAAAAAA = all ascending
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.ASCENDING),
        new KeyColumn("6", KeyOrder.ASCENDING),
        new KeyColumn("7", KeyOrder.ASCENDING),
        new KeyColumn("8", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE),
        sortUsingByteKeyComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE)
    );
  }

  @Test
  public void test_compare_ADDADDAA()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.DESCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.DESCENDING),
        new KeyColumn("6", KeyOrder.DESCENDING),
        new KeyColumn("7", KeyOrder.ASCENDING),
        new KeyColumn("8", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE),
        sortUsingByteKeyComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE)
    );
  }

  @Test
  public void test_compare_DAADAADD()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.DESCENDING),
        new KeyColumn("5", KeyOrder.ASCENDING),
        new KeyColumn("6", KeyOrder.ASCENDING),
        new KeyColumn("7", KeyOrder.DESCENDING),
        new KeyColumn("8", KeyOrder.DESCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE),
        sortUsingByteKeyComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE)
    );
  }

  @Test
  public void test_compare_DADADADA()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.DESCENDING),
        new KeyColumn("6", KeyOrder.ASCENDING),
        new KeyColumn("7", KeyOrder.DESCENDING),
        new KeyColumn("8", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE),
        sortUsingByteKeyComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE)
    );
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(ByteRowKeyComparator.class)
                  .usingGetClass()
                  .verify();
  }

  private static List<RowKey> sortUsingByteKeyComparator(
      final List<KeyColumn> keyColumns,
      final List<Object[]> objectss,
      final RowSignature rowSignature
  )
  {
    return objectss.stream()
                   .map(objects -> KeyTestUtils.createKey(rowSignature, objects).array())
                   .sorted(ByteRowKeyComparator.create(keyColumns, rowSignature))
                   .map(RowKey::wrap)
                   .collect(Collectors.toList());
  }

  private static List<RowKey> sortUsingObjectComparator(
      final List<KeyColumn> keyColumns,
      final List<Object[]> objectss,
      final RowSignature rowSignature
  )
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
      sortedKeys.add(KeyTestUtils.createKey(rowSignature, objects));
    }

    return sortedKeys;
  }

  public static HyperLogLogCollector makeHllCollector(final int estimatedCardinality)
  {
    final HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    for (int i = 0; i < estimatedCardinality; ++i) {
      collector.add(Hashing.murmur3_128().hashBytes(StringUtils.toUtf8(String.valueOf(i))).asBytes());
    }

    return collector;
  }
}
