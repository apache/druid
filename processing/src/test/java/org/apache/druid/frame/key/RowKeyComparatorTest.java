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
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.druid.frame.key.ByteRowKeyComparatorTest.ALL_KEY_OBJECTS;
import static org.apache.druid.frame.key.ByteRowKeyComparatorTest.KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN;
import static org.apache.druid.frame.key.ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE;
import static org.apache.druid.frame.key.ByteRowKeyComparatorTest.SIGNATURE;

public class RowKeyComparatorTest extends InitializedNullHandlingTest
{
  static {
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());
  }

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
        sortUsingKeyComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE)
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
        sortUsingKeyComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE)
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
        sortUsingKeyComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE)
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
        sortUsingKeyComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE)
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
        sortUsingKeyComparator(keyColumns, KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN, NO_COMPLEX_SIGNATURE)
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
        sortUsingKeyComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE)
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
        sortUsingKeyComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE)
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
        sortUsingKeyComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE)
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
        sortUsingKeyComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE)
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
        sortUsingKeyComparator(keyColumns, ALL_KEY_OBJECTS, SIGNATURE)
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

  private static List<RowKey> sortUsingKeyComparator(
      final List<KeyColumn> keyColumns,
      final List<Object[]> objectss,
      final RowSignature rowSignature
  )
  {
    final List<RowKey> sortedKeys = new ArrayList<>();

    for (final Object[] objects : objectss) {
      sortedKeys.add(KeyTestUtils.createKey(rowSignature, objects));
    }

    sortedKeys.sort(RowKeyComparator.create(keyColumns, rowSignature));
    return sortedKeys;
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
}
