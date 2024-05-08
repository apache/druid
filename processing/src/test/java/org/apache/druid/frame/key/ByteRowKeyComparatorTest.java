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

  static final RowSignature SIGNATURE =
      RowSignature.builder()
                  .add("1", ColumnType.LONG)
                  .add("2", ColumnType.STRING)
                  .add("3", ColumnType.LONG)
                  .add("4", ColumnType.DOUBLE)
                  .add("5", HyperUniquesAggregatorFactory.TYPE)
                  .build();
  private static final Object[] OBJECTS1 = new Object[]{-1L, "foo", 2L, -1.2, makeHllCollector(5)};
  private static final Object[] OBJECTS2 = new Object[]{-1L, null, 2L, 1.2d, makeHllCollector(50)};
  private static final Object[] OBJECTS3 = new Object[]{-1L, "bar", 2L, 1.2d, makeHllCollector(5)};
  private static final Object[] OBJECTS4 = new Object[]{-1L, "foo", 2L, 1.2d, makeHllCollector(1)};
  private static final Object[] OBJECTS5 = new Object[]{-1L, "foo", 3L, 1.2d, makeHllCollector(50)};
  private static final Object[] OBJECTS6 = new Object[]{-1L, "foo", 2L, 1.3d, makeHllCollector(100)};
  private static final Object[] OBJECTS7 = new Object[]{1L, "foo", 2L, -1.2d, makeHllCollector(5)};
  private static final Object[] OBJECTS8 = new Object[]{1L, "foo", 2L, -1.2d, makeHllCollector(500)};

  static final List<Object[]> ALL_KEY_OBJECTS = Arrays.asList(
      OBJECTS1,
      OBJECTS2,
      OBJECTS3,
      OBJECTS4,
      OBJECTS5,
      OBJECTS6,
      OBJECTS7,
      OBJECTS8
  );

  @Test
  public void test_compare_DDDDD() // DDDDD = all descending
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.DESCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.DESCENDING),
        new KeyColumn("5", KeyOrder.DESCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS),
        sortUsingByteKeyComparator(keyColumns, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_AAAAA() // AAAAA = all ascending
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS),
        sortUsingByteKeyComparator(keyColumns, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_ADDAD()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.DESCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.DESCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS),
        sortUsingByteKeyComparator(keyColumns, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_DAADA()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.DESCENDING),
        new KeyColumn("5", KeyOrder.ASCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS),
        sortUsingByteKeyComparator(keyColumns, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_DADAD()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.DESCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.DESCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.DESCENDING)
    );
    Assert.assertEquals(
        sortUsingObjectComparator(keyColumns, ALL_KEY_OBJECTS),
        sortUsingByteKeyComparator(keyColumns, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(ByteRowKeyComparator.class)
                  .usingGetClass()
                  .verify();
  }

  private List<RowKey> sortUsingByteKeyComparator(final List<KeyColumn> keyColumns, final List<Object[]> objectss)
  {
    return objectss.stream()
                   .map(objects -> KeyTestUtils.createKey(SIGNATURE, objects).array())
                   .sorted(ByteRowKeyComparator.create(keyColumns, SIGNATURE))
                   .map(RowKey::wrap)
                   .collect(Collectors.toList());
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

  private static HyperLogLogCollector makeHllCollector(final int estimatedCardinality)
  {
    final HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    for (int i = 0; i < estimatedCardinality; ++i) {
      collector.add(Hashing.murmur3_128().hashBytes(StringUtils.toUtf8(String.valueOf(i))).asBytes());
    }

    return collector;
  }
}
