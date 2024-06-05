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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
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
    Assert.assertEquals(0, runLengths.getRunLengthEntries().length);
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
      Assert.assertEquals(1, runLengths.getRunLengthEntries().length);
      Assert.assertTrue(runLengths.getRunLengthEntries()[0].isByteComparable());
      Assert.assertEquals(1, runLengths.getRunLengthEntries()[0].getRunLength());
      Assert.assertEquals(KeyOrder.ASCENDING, runLengths.getRunLengthEntries()[0].getOrder());
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
      Assert.assertEquals(1, runLengths.getRunLengthEntries().length);
      Assert.assertFalse(runLengths.getRunLengthEntries()[0].isByteComparable());
      Assert.assertEquals(1, runLengths.getRunLengthEntries()[0].getRunLength());
      Assert.assertEquals(KeyOrder.ASCENDING, runLengths.getRunLengthEntries()[0].getOrder());
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

    final RunLengthEntry[] runLengthEntries =
        RowKeyComparisonRunLengths.create(keyColumns, signature).getRunLengthEntries();

    // Input keyColumns
    // long ASC, string ASC, string DESC, long DESC, complex DESC, complex ASC, complex ASC, string ASC

    // Output runLengthEntries would be
    // (long, string ASC) (string, long DESC) (complex DESC) (complex ASC) (complex ASC) (string ASC)

    Assert.assertEquals(6, runLengthEntries.length);

    Assert.assertTrue(runLengthEntries[0].isByteComparable());
    Assert.assertEquals(2, runLengthEntries[0].getRunLength());
    Assert.assertEquals(KeyOrder.ASCENDING, runLengthEntries[0].getOrder());

    Assert.assertTrue(runLengthEntries[1].isByteComparable());
    Assert.assertEquals(2, runLengthEntries[1].getRunLength());
    Assert.assertEquals(KeyOrder.DESCENDING, runLengthEntries[1].getOrder());

    Assert.assertFalse(runLengthEntries[2].isByteComparable());
    Assert.assertEquals(1, runLengthEntries[2].getRunLength());
    Assert.assertEquals(KeyOrder.DESCENDING, runLengthEntries[2].getOrder());

    Assert.assertFalse(runLengthEntries[3].isByteComparable());
    Assert.assertEquals(1, runLengthEntries[3].getRunLength());
    Assert.assertEquals(KeyOrder.ASCENDING, runLengthEntries[3].getOrder());

    Assert.assertFalse(runLengthEntries[4].isByteComparable());
    Assert.assertEquals(1, runLengthEntries[4].getRunLength());
    Assert.assertEquals(KeyOrder.ASCENDING, runLengthEntries[4].getOrder());

    Assert.assertTrue(runLengthEntries[5].isByteComparable());
    Assert.assertEquals(1, runLengthEntries[5].getRunLength());
    Assert.assertEquals(KeyOrder.ASCENDING, runLengthEntries[5].getOrder());
  }

  /**
   * This tests the creation of the run lengths with all the permutations of the key columns from the following space:
   * a. The KeyColumn can be either String or Complex (string is byte-comparable, nested data is not)
   * b. The KeyColumn can be either ASC or DESC
   *
   * Therefore, each key column can be one of (string ASC, string DESC, complex ASC, complex DESC). There are 64 test
   * case for all the permutations of the key columns, because there are 3 key columns, each of which can take one of
   * the 4 different configurations..
   *
   * Test cases are generated programatically. For index i from [0..64), we build the base-4 representation of the index,
   * and each digit in the representation corresponds to one of the key columns.
   */
  @Test
  public void testRunLengthsWithAllPermutationsOfThreeLengthKeyColumns()
  {

    ImmutableList.Builder<RunLengthEntry[]> expectedResultsBuilder = ImmutableList.builder();

    // index = 0; KeyColumns = STRING ASC, STRING ASC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 3)
        }
    );

    // index = 1; KeyColumns = STRING DESC, STRING ASC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 2)
        }
    );

    // index = 2; KeyColumns = COMPLEX ASC, STRING ASC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 2)
        }
    );

    // index = 3; KeyColumns = COMPLEX DESC, STRING ASC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 2)
        }
    );

    // index = 4; KeyColumns = STRING ASC, STRING DESC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 5; KeyColumns = STRING DESC, STRING DESC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 2),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 6; KeyColumns = COMPLEX ASC, STRING DESC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 7; KeyColumns = COMPLEX DESC, STRING DESC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 8; KeyColumns = STRING ASC, COMPLEX ASC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 9; KeyColumns = STRING DESC, COMPLEX ASC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 10; KeyColumns = COMPLEX ASC, COMPLEX ASC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
        }
    );

    // index = 11; KeyColumns = COMPLEX DESC, COMPLEX ASC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 12; KeyColumns = STRING ASC, COMPLEX DESC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 13; KeyColumns = STRING DESC, COMPLEX DESC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 14; KeyColumns = COMPLEX ASC, COMPLEX DESC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            }
    );

    // index = 15; KeyColumns = COMPLEX DESC, COMPLEX DESC, STRING ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 16; KeyColumns = STRING ASC, STRING ASC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 2),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
        }
    );

    // index = 17; KeyColumns = STRING DESC, STRING ASC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 18; KeyColumns = COMPLEX ASC, STRING ASC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 19; KeyColumns = COMPLEX DESC, STRING ASC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 20; KeyColumns = STRING ASC, STRING DESC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 2)
        }
    );

    // index = 21; KeyColumns = STRING DESC, STRING DESC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 3)
        }
    );

    // index = 22; KeyColumns = COMPLEX ASC, STRING DESC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 2)
        }
    );

    // index = 23; KeyColumns = COMPLEX DESC, STRING DESC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 2)
        }
    );

    // index = 24; KeyColumns = STRING ASC, COMPLEX ASC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 25; KeyColumns = STRING DESC, COMPLEX ASC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 26; KeyColumns = COMPLEX ASC, COMPLEX ASC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            }
    );

    // index = 27; KeyColumns = COMPLEX DESC, COMPLEX ASC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 28; KeyColumns = STRING ASC, COMPLEX DESC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 29; KeyColumns = STRING DESC, COMPLEX DESC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 30; KeyColumns = COMPLEX ASC, COMPLEX DESC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            }
    );

    // index = 31; KeyColumns = COMPLEX DESC, COMPLEX DESC, STRING DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 32; KeyColumns = STRING ASC, STRING ASC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 2),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 33; KeyColumns = STRING DESC, STRING ASC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 34; KeyColumns = COMPLEX ASC, STRING ASC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 35; KeyColumns = COMPLEX DESC, STRING ASC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 36; KeyColumns = STRING ASC, STRING DESC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 37; KeyColumns = STRING DESC, STRING DESC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 2),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 38; KeyColumns = COMPLEX ASC, STRING DESC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 39; KeyColumns = COMPLEX DESC, STRING DESC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 40; KeyColumns = STRING ASC, COMPLEX ASC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 41; KeyColumns = STRING DESC, COMPLEX ASC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 42; KeyColumns = COMPLEX ASC, COMPLEX ASC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            }
    );

    // index = 43; KeyColumns = COMPLEX DESC, COMPLEX ASC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 44; KeyColumns = STRING ASC, COMPLEX DESC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 45; KeyColumns = STRING DESC, COMPLEX DESC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );

    // index = 46; KeyColumns = COMPLEX ASC, COMPLEX DESC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            }
    );

    // index = 47; KeyColumns = COMPLEX DESC, COMPLEX DESC, COMPLEX ASC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1)
        }
    );


    // index = 48; KeyColumns = STRING ASC, STRING ASC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 2),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 49; KeyColumns = STRING DESC, STRING ASC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 50; KeyColumns = COMPLEX ASC, STRING ASC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 51; KeyColumns = COMPLEX DESC, STRING ASC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 52; KeyColumns = STRING ASC, STRING DESC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 53; KeyColumns = STRING DESC, STRING DESC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 2),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 54; KeyColumns = COMPLEX ASC, STRING DESC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 55; KeyColumns = COMPLEX DESC, STRING DESC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 56; KeyColumns = STRING ASC, COMPLEX ASC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 57; KeyColumns = STRING DESC, COMPLEX ASC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 58; KeyColumns = COMPLEX ASC, COMPLEX ASC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            }
    );

    // index = 59; KeyColumns = COMPLEX DESC, COMPLEX ASC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 60; KeyColumns = STRING ASC, COMPLEX DESC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 61; KeyColumns = STRING DESC, COMPLEX DESC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(true, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    // index = 62; KeyColumns = COMPLEX ASC, COMPLEX DESC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.ASCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            }
    );

    // index = 63; KeyColumns = COMPLEX DESC, COMPLEX DESC, COMPLEX DESC
    expectedResultsBuilder.add(
        new RunLengthEntry[]{
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1),
            new RunLengthEntry(false, KeyOrder.DESCENDING, 1)
        }
    );

    List<RunLengthEntry[]> expectedResults = expectedResultsBuilder.build();

    final List<Pair<ColumnType, KeyOrder>> columnTypeAndKeyOrder = ImmutableList.of(
        Pair.of(ColumnType.STRING, KeyOrder.ASCENDING),
        Pair.of(ColumnType.STRING, KeyOrder.DESCENDING),
        Pair.of(ColumnType.NESTED_DATA, KeyOrder.ASCENDING),
        Pair.of(ColumnType.NESTED_DATA, KeyOrder.DESCENDING)
    );


    for (int i = 0; i < 64; ++i) {
      Pair<List<KeyColumn>, RowSignature> keyColumnsAndRowSignature = generateKeyColumns(columnTypeAndKeyOrder, i);
      RunLengthEntry[] actualEntries = RowKeyComparisonRunLengths
          .create(keyColumnsAndRowSignature.lhs, keyColumnsAndRowSignature.rhs)
          .getRunLengthEntries();
      Assert.assertArrayEquals(StringUtils.format("Result %d incorrect", i), expectedResults.get(i), actualEntries);
    }
  }

  private Pair<List<KeyColumn>, RowSignature> generateKeyColumns(
      final List<Pair<ColumnType, KeyOrder>> columnTypeAndKeyOrder,
      int index
  )
  {
    final List<KeyColumn> keyColumns = new ArrayList<>();
    final RowSignature.Builder builder = RowSignature.builder();

    int firstKeyColumn = index % 4;
    keyColumns.add(new KeyColumn("a", columnTypeAndKeyOrder.get(firstKeyColumn).rhs));
    builder.add("a", columnTypeAndKeyOrder.get(firstKeyColumn).lhs);
    index /= 4;

    int secondKeyColumn = index % 4;
    keyColumns.add(new KeyColumn("b", columnTypeAndKeyOrder.get(secondKeyColumn).rhs));
    builder.add("b", columnTypeAndKeyOrder.get(secondKeyColumn).lhs);
    index /= 4;

    int thirdKeyColumn = index % 4; // Should be no-op, since index < 64
    keyColumns.add(new KeyColumn("c", columnTypeAndKeyOrder.get(thirdKeyColumn).rhs));
    builder.add("c", columnTypeAndKeyOrder.get(thirdKeyColumn).lhs);

    return Pair.of(keyColumns, builder.build());
  }
}
