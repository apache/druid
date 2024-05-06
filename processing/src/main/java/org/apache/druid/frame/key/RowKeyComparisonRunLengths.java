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

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class RowKeyComparisonRunLengths
{

  public static RowKeyComparisonRunLengths create(final List<KeyColumn> keyColumns, RowSignature rowSignature)
  {
    final List<RunLengthEntry> runLengthEntries = new ArrayList<>();
    for (KeyColumn keyColumn : keyColumns) {

      if (keyColumn.order() == KeyOrder.NONE) {
        throw DruidException.defensive(
            "Cannot sort on column [%s] when the sorting order isn't provided",
            keyColumn.columnName()
        );
      }

      ColumnType columnType = rowSignature.getColumnType(keyColumn.columnName())
                                          .orElseThrow(() -> DruidException.defensive("Need column types"));

      if (runLengthEntries.size() == 0) {
        runLengthEntries.add(
            new RunLengthEntry(
                isByteComparable(columnType),
                1,
                keyColumn.order()
            )
        );
        continue;
      }

      // There is atleast one RunLengthEntry present in the array. Check if we can find a way to merge the current entry
      // with the previous one
      boolean isCurrentColumnByteComparable = isByteComparable(columnType);
      RunLengthEntry lastRunLengthEntry = runLengthEntries.get(runLengthEntries.size() - 1);
      if (lastRunLengthEntry.isByteComparable()
          && isCurrentColumnByteComparable
          && lastRunLengthEntry.order.equals(keyColumn.order())
      ) {
        lastRunLengthEntry.runLength++;
      } else {
        runLengthEntries.add(
            new RunLengthEntry(
                isCurrentColumnByteComparable,
                1,
                keyColumn.order()
            )
        );
      }
    }
    return new RowKeyComparisonRunLengths(runLengthEntries);
  }

  public static boolean isByteComparable(@Nullable ColumnType columnType)
  {
    // For backward compatibility
    // Only types which were byte comparable were allowed. Therefore, if we don't know the columnType, we should assume
    // that it is byte comparable
    if (columnType == null) {
      return true;
    } else if (columnType.is(ValueType.COMPLEX)) {
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

  private final List<RunLengthEntry> runLengthEntries;

  public RowKeyComparisonRunLengths(List<RunLengthEntry> runLengthEntries)
  {
    this.runLengthEntries = runLengthEntries;
  }

  public List<RunLengthEntry> getRunLengthEntries()
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
    return Objects.equals(runLengthEntries, that.runLengthEntries);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(runLengthEntries);
  }

  @Override
  public String toString()
  {
    return runLengthEntries.toString();
  }

  /**
   * Information about a continguous run of keys, that has the same sorting order
   */
  public static class RunLengthEntry
  {
    private final boolean byteComparable;
    private int runLength;
    private final KeyOrder order;

    private RunLengthEntry(boolean byteComparable, int runLength, KeyOrder order)
    {
      this.byteComparable = byteComparable;
      this.runLength = runLength;
      this.order = order;
    }

    public boolean isByteComparable()
    {
      return byteComparable;
    }

    public int getRunLength()
    {
      return runLength;
    }

    public KeyOrder getOrder()
    {
      return order;
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
      RunLengthEntry that = (RunLengthEntry) o;
      return byteComparable == that.byteComparable && runLength == that.runLength && order == that.order;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(byteComparable, runLength, order);
    }

    @Override
    public String toString()
    {
      return "RunLengthEntry{" +
             "byteComparable=" + byteComparable +
             ", runLength=" + runLength +
             ", order=" + order +
             '}';
    }
  }
}
