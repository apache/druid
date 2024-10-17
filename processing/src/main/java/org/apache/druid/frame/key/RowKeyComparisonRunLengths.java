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

import org.apache.druid.error.DruidException;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Denotes the ascending-descending run lengths of the fields of the keycolumns that can be compared together.
 * It analyses the key columns and their types. It coalesces the adjacent key columns if they are:
 * a. Byte comparable, i.e. the fields won't need to be deserialized before comparing. It doesn't care about the types
 * b. Have same order
 *
 * All the primitive and the primitive arrays are byte comparable. The complex types are not byte comparable, and nested arrays
 * and arrays of complex objects are not supported by MSQ right now.
 *
 * Consider a row with the key columns like:
 *
 * ColumnName       ColumnType        Order
 * ==========================================
 * longAsc1         LONG              ASC
 * stringAsc1       STRING            ASC
 * stringDesc1      STRING            DESC
 * longDesc1        LONG              DESC
 * complexDesc1     COMPLEX           DESC
 * complexAsc1      COMPLEX           ASC
 * complexAsc2      COMPLEX           ASC
 * stringAsc2       STRING            ASC
 *
 * The run lengths generated would be:
 *
 * RunLengthEntry        Run length   Order     Is byte comparable    Explanation
 * ====================================================================================================================
 * RunLengthEntry#1      2            ASC       true                  Even though longAsc1 and stringAsc1 had different types,
 *                                                                    both types are byte comparable and have same direction. Therefore,
 *                                                                    they can be byte-compared together
 *
 * RunLengthEntry#2      2            DESC      true                  stringDesc1 can't be clubed with the previous stringAsc1 due to
 *                                                                    different ordering. It is clubbed with the following longDesc1 due
 *                                                                    to the reason stated above
 * RunLengthEntry#3      1            DESC      false                 Non byte comparable types cannot be clubbed with anything
 * RunLengthEntry#4      1            ASC       false                 Non byte comparable types cannot be clubbed with anything
 * RunLengthEntry#5      1            ASC       false                 Non byte comparable types cannot be clubbed with anything despite
 *                                                                    the previous key column having same order and the type
 * RunLengthEntry#6      1            ASC       true                  Cannot be clubbed with previous entry. It is own entry
 *
 */
public class RowKeyComparisonRunLengths
{
  private final RunLengthEntry[] runLengthEntries;

  private RowKeyComparisonRunLengths(final RunLengthEntry[] runLengthEntries)
  {
    this.runLengthEntries = runLengthEntries;
  }

  public static RowKeyComparisonRunLengths create(final List<KeyColumn> keyColumns, RowSignature rowSignature)
  {
    final List<RunLengthEntryBuilder> runLengthEntryBuilders = new ArrayList<>();
    for (KeyColumn keyColumn : keyColumns) {
      if (keyColumn.order() == KeyOrder.NONE) {
        throw DruidException.defensive(
            "Cannot sort on column [%s] when the sorting order isn't provided",
            keyColumn.columnName()
        );
      }

      ColumnType columnType =
          rowSignature.getColumnType(keyColumn.columnName())
                      .orElseThrow(() -> DruidException.defensive("No type for column[%s]", keyColumn.columnName()));

      // First key column to be processed
      if (runLengthEntryBuilders.isEmpty()) {
        final boolean isByteComparable = isByteComparable(columnType);
        runLengthEntryBuilders.add(
            new RunLengthEntryBuilder(isByteComparable, keyColumn.order())
        );
        continue;
      }

      // There is atleast one RunLengthEntry present in the array. Check if we can find a way to merge the current entry
      // with the previous one
      boolean isCurrentColumnByteComparable = isByteComparable(columnType);
      RunLengthEntryBuilder lastRunLengthEntryBuilder = runLengthEntryBuilders.get(runLengthEntryBuilders.size() - 1);
      if (lastRunLengthEntryBuilder.byteComparable
          && isCurrentColumnByteComparable
          && lastRunLengthEntryBuilder.order.equals(keyColumn.order())
      ) {
        lastRunLengthEntryBuilder.runLength++;
      } else {
        runLengthEntryBuilders.add(
            new RunLengthEntryBuilder(
                isCurrentColumnByteComparable,
                keyColumn.order()
            )
        );
      }
    }

    RunLengthEntry[] runLengthEntries = new RunLengthEntry[runLengthEntryBuilders.size()];
    for (int i = 0; i < runLengthEntryBuilders.size(); ++i) {
      runLengthEntries[i] = runLengthEntryBuilders.get(i).build();
    }

    return new RowKeyComparisonRunLengths(runLengthEntries);
  }

  private static boolean isByteComparable(final ColumnType columnType)
  {
    if (columnType.is(ValueType.COMPLEX)) {
      if (columnType.getComplexTypeName() == null) {
        throw DruidException.defensive("Cannot sort unknown complex types");
      }
      // Complex types with known types are not byte comparable and must be deserialized for comparison
      return false;
    } else if (columnType.isArray() && !columnType.isPrimitiveArray()) {
      // Nested arrays aren't allowed directly in the frames - they are materialized as nested types.
      // Nested arrays aren't byte comparable, if they find a way to creep in.
      throw DruidException.defensive("Nested arrays aren't supported in row based frames");
    }
    return true;
  }

  public RunLengthEntry[] getRunLengthEntries()
  {
    return runLengthEntries;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowKeyComparisonRunLengths that = (RowKeyComparisonRunLengths) o;
    return Arrays.equals(runLengthEntries, that.runLengthEntries);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(runLengthEntries);
  }

  @Override
  public String toString()
  {
    return "RowKeyComparisonRunLengths{" +
           "runLengthEntries=" + Arrays.toString(runLengthEntries) +
           '}';
  }

  /**
   * Builder for {@link RunLengthEntry}. Contains mutable state, therefore it isn't suitable for equality and hashCode.
   */
  private static class RunLengthEntryBuilder
  {
    private final boolean byteComparable;
    private final KeyOrder order;
    private int runLength;

    public RunLengthEntryBuilder(
        final boolean byteComparable,
        final KeyOrder order
    )
    {
      this.byteComparable = byteComparable;
      this.order = order;
      this.runLength = 1;
    }

    public RunLengthEntry build()
    {
      return new RunLengthEntry(byteComparable, order, runLength);
    }
  }
}
