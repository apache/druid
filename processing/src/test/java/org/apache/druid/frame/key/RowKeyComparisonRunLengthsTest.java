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
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class RowKeyComparisonRunLengthsTest
{

  @Test
  public void testRunLengthsWithNoKeyColumns()
  {
    final List<KeyColumn> keyColumns = Collections.emptyList();
    final RowSignature signature = RowSignature.empty();
    final RowKeyComparisonRunLengths runLengths = RowKeyComparisonRunLengths.create(keyColumns, signature);
    Assert.assertEquals(0, runLengths.getRunLengthEntries().size());
  }

  @Test
  public void testRunLengthsWithInvalidOrder()
  {
    final List<KeyColumn> keyColumns = Collections.singletonList(new KeyColumn("a", KeyOrder.NONE));
    final RowSignature signature = RowSignature.builder()
                                         .add("a", ColumnType.LONG)
                                         .build();
    Assert.assertThrows(DruidException.class, () -> RowKeyComparisonRunLengths.create(keyColumns, signature));
  }

  @Test
  public void testRunLengthsWithIncompleteRowSignature()
  {
    final List<KeyColumn> keyColumns = Collections.singletonList(new KeyColumn("a", KeyOrder.NONE));
    final RowSignature signature = RowSignature.empty();
    Assert.assertThrows(DruidException.class, () -> RowKeyComparisonRunLengths.create(keyColumns, signature));
  }

  @Test
  public void testRunLengthsWithEmptyType()
  {
    final List<KeyColumn> keyColumns = Collections.singletonList(new KeyColumn("a", KeyOrder.NONE));
    final RowSignature signature1 = RowSignature.builder()
                                         .add("a", null)
                                         .build();
    Assert.assertThrows(DruidException.class, () -> RowKeyComparisonRunLengths.create(keyColumns, signature1));

    final RowSignature signature2 = RowSignature.builder()
                                                .add("a", ColumnType.UNKNOWN_COMPLEX)
                                                .build();
    Assert.assertThrows(DruidException.class, () -> RowKeyComparisonRunLengths.create(keyColumns, signature2));
  }

  @Test
  public void testRunLengthsWithByteComparableTypes()
  {
    final List<KeyColumn> keyColumns = Collections.singletonList(new KeyColumn("a", KeyOrder.ASCENDING));
    final List<ColumnType> byteComparableTypes = ImmutableList.of(
        ColumnType.LONG,
        ColumnType.FLOAT,
        ColumnType.DOUBLE,
        ColumnType.STRING,
        ColumnType.LONG_ARRAY,
        ColumnType.FLOAT_ARRAY,
        ColumnType.DOUBLE_ARRAY,
        ColumnType.STRING_ARRAY
    );

    for (final ColumnType columnType : byteComparableTypes) {
      final RowSignature signature = RowSignature.builder()
                                                 .add("a", columnType)
                                                 .build();
      final RowKeyComparisonRunLengths runLengths = RowKeyComparisonRunLengths.create(keyColumns, signature);
      Assert.assertEquals(1, runLengths.getRunLengthEntries().size());
      Assert.assertTrue(runLengths.getRunLengthEntries().get(0).isByteComparable());
      Assert.assertEquals(1, runLengths.getRunLengthEntries().get(0).getRunLength());
      Assert.assertEquals(KeyOrder.ASCENDING, runLengths.getRunLengthEntries().get(0).getOrder());
    }
  }

  @Test
  public void testRunLengthsWithNonByteComparableTypes()
  {
    final List<KeyColumn> keyColumns = Collections.singletonList(new KeyColumn("a", KeyOrder.ASCENDING));
    // Any known complex type
    final List<ColumnType> byteComparableTypes = ImmutableList.of(ColumnType.NESTED_DATA);

    for (final ColumnType columnType : byteComparableTypes) {
      final RowSignature signature = RowSignature.builder()
                                                 .add("a", columnType)
                                                 .build();
      final RowKeyComparisonRunLengths runLengths = RowKeyComparisonRunLengths.create(keyColumns, signature);
      Assert.assertEquals(1, runLengths.getRunLengthEntries().size());
      Assert.assertFalse(runLengths.getRunLengthEntries().get(0).isByteComparable());
      Assert.assertEquals(1, runLengths.getRunLengthEntries().get(0).getRunLength());
      Assert.assertEquals(KeyOrder.ASCENDING, runLengths.getRunLengthEntries().get(0).getOrder());
    }
  }

  @Test
  public void testRunLengthsWithMultipleColumns()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("longAsc1", KeyOrder.ASCENDING),
        new KeyColumn("stringAsc1", KeyOrder.ASCENDING),
        new KeyColumn("stringDesc1", KeyOrder.DESCENDING),
        new KeyColumn("longDesc1", KeyOrder.DESCENDING),
        new KeyColumn("complexDesc1", KeyOrder.DESCENDING),
        new KeyColumn("complexAsc1", KeyOrder.ASCENDING),
        new KeyColumn("complexAsc2", KeyOrder.ASCENDING),
        new KeyColumn("stringAsc2", KeyOrder.ASCENDING)
    );

    final RowSignature signature = RowSignature.builder()
                                               .add("longAsc1", ColumnType.LONG)
                                               .add("stringAsc1", ColumnType.STRING)
                                               .add("stringDesc1", ColumnType.STRING)
                                               .add("longDesc1", ColumnType.LONG)
                                               .add("complexDesc1", ColumnType.NESTED_DATA)
                                               .add("complexAsc1", ColumnType.NESTED_DATA)
                                               .add("complexAsc2", ColumnType.NESTED_DATA)
                                               .add("stringAsc2", ColumnType.STRING)
                                               .build();

    final List<RowKeyComparisonRunLengths.RunLengthEntry> runLengthEntries =
        RowKeyComparisonRunLengths.create(keyColumns, signature).getRunLengthEntries();

    // Input keyColumns
    // long ASC, string ASC, string DESC, long DESC, complex DESC, complex ASC, complex ASC, string ASC

    // Output runLengthEntries would be
    // (long, string ASC) (string, long DESC) (complex DESC) (complex ASC) (complex ASC) (string ASC)

    Assert.assertEquals(6, runLengthEntries.size());

    Assert.assertTrue(runLengthEntries.get(0).isByteComparable());
    Assert.assertEquals(2, runLengthEntries.get(0).getRunLength());
    Assert.assertEquals(KeyOrder.ASCENDING, runLengthEntries.get(0).getOrder());

    Assert.assertTrue(runLengthEntries.get(1).isByteComparable());
    Assert.assertEquals(2, runLengthEntries.get(1).getRunLength());
    Assert.assertEquals(KeyOrder.DESCENDING, runLengthEntries.get(1).getOrder());

    Assert.assertFalse(runLengthEntries.get(2).isByteComparable());
    Assert.assertEquals(1, runLengthEntries.get(2).getRunLength());
    Assert.assertEquals(KeyOrder.DESCENDING, runLengthEntries.get(2).getOrder());

    Assert.assertFalse(runLengthEntries.get(3).isByteComparable());
    Assert.assertEquals(1, runLengthEntries.get(3).getRunLength());
    Assert.assertEquals(KeyOrder.ASCENDING, runLengthEntries.get(3).getOrder());

    Assert.assertFalse(runLengthEntries.get(4).isByteComparable());
    Assert.assertEquals(1, runLengthEntries.get(4).getRunLength());
    Assert.assertEquals(KeyOrder.ASCENDING, runLengthEntries.get(4).getOrder());

    Assert.assertTrue(runLengthEntries.get(5).isByteComparable());
    Assert.assertEquals(1, runLengthEntries.get(5).getRunLength());
    Assert.assertEquals(KeyOrder.ASCENDING, runLengthEntries.get(5).getOrder());

  }

}