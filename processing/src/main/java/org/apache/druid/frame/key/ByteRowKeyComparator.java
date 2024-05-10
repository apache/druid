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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Comparator for byte arrays from {@link RowKey#key} instances.
 *
 * Comparison logic in this class is very similar to {@link FrameComparisonWidget}, but is different because it works
 * on byte[] instead of Frames.
 */
public class ByteRowKeyComparator implements Comparator<byte[]>
{
  private final List<KeyColumn> keyColumns;
  private final int firstFieldPosition;
  private final RowKeyComparisonRunLengths rowKeyComparisonRunLengths;
  private final RowSignature rowSignature;
  private final ComplexMetricSerde[] complexMetricSerdes;
  private final ColumnType[] columnTypes;

  private ByteRowKeyComparator(
      final List<KeyColumn> keyColumns,
      final RowKeyComparisonRunLengths rowKeyComparisonRunLengths,
      final RowSignature rowSignature,
      final ComplexMetricSerde[] complexMetricSerdes,
      final ColumnType[] columnTypes
  )
  {
    this.keyColumns = keyColumns;
    this.firstFieldPosition = computeFirstFieldPosition(keyColumns.size());
    this.rowKeyComparisonRunLengths = rowKeyComparisonRunLengths;
    this.rowSignature = relevantRowSignature(keyColumns, rowSignature);
    this.complexMetricSerdes = complexMetricSerdes;
    this.columnTypes = columnTypes;
  }

  // Trims down the rowSignature to relevant portion
  private static RowSignature relevantRowSignature(final List<KeyColumn> keyColumns, final RowSignature completeRowSignature)
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (final KeyColumn keyColumn : keyColumns) {
      builder.add(
          keyColumn.columnName(),
          completeRowSignature.getColumnType(keyColumn.columnName())
                              .orElseThrow(() -> DruidException.defensive("Expected type from the comparison key"))
      );
    }
    return builder.build();
  }

  public static ByteRowKeyComparator create(final List<KeyColumn> keyColumns, final RowSignature rowSignature)
  {
    final RowKeyComparisonRunLengths rowKeyComparisonRunLengths = RowKeyComparisonRunLengths.create(
        keyColumns,
        rowSignature
    );
    final RunLengthEntry[] runLengthEntries = rowKeyComparisonRunLengths.getRunLengthEntries();
    final ComplexMetricSerde[] complexMetricSerdes = new ComplexMetricSerde[runLengthEntries.length];
    final ColumnType[] columnTypes = new ColumnType[runLengthEntries.length];

    int fieldsSeenSoFar = 0;

    for (int i = 0; i < runLengthEntries.length; ++i) {
      if (runLengthEntries[i].isByteComparable()) {
        complexMetricSerdes[i] = null;
        columnTypes[i] = null;
      } else {
        final ColumnType columnType = rowSignature.getColumnType(keyColumns.get(fieldsSeenSoFar).columnName())
                                                  .orElse(null);
        if (columnType == null) {
          throw DruidException.defensive("Expected column type for comparison");
        }
        final String complexTypeName = columnType.getComplexTypeName();
        if (complexTypeName == null) {
          throw DruidException.defensive("Expected complex type name for comparison");
        }

        complexMetricSerdes[i] = Preconditions.checkNotNull(
            ComplexMetrics.getSerdeForType(complexTypeName),
            "Cannot find serde for type [%s]",
            complexTypeName
        );
        columnTypes[i] = columnType;
      }

      fieldsSeenSoFar += runLengthEntries[i].getRunLength();
    }

    return new ByteRowKeyComparator(
        keyColumns,
        RowKeyComparisonRunLengths.create(keyColumns, rowSignature),
        rowSignature,
        complexMetricSerdes,
        columnTypes
    );
  }

  /**
   * Compute the offset into each key where the first field starts.
   *
   * Public so {@link FrameComparisonWidgetImpl} can use it.
   */
  public static int computeFirstFieldPosition(final int fieldCount)
  {
    return Ints.checkedCast((long) fieldCount * Integer.BYTES);
  }

  @Override
  @SuppressWarnings("SubtractionInCompareTo")
  public int compare(final byte[] keyArray1, final byte[] keyArray2)
  {
    // Similar logic to FrameComparisonWidgetImpl, but implementation is different enough that we need our own.
    // Major difference is Frame v. Frame instead of byte[] v. byte[].

    int currentRunStartPosition1 = firstFieldPosition;
    int currentRunStartPosition2 = firstFieldPosition;

    // Number of fields compared till now, which is equivalent to the index of the field to compare next
    int fieldsComparedTillNow = 0;

    for (int i = 0; i < rowKeyComparisonRunLengths.getRunLengthEntries().length; ++i) {
      final RunLengthEntry runLengthEntry = rowKeyComparisonRunLengths.getRunLengthEntries()[i];

      if (runLengthEntry.getRunLength() <= 0) {
        // Defensive check
        continue;
      }

      // Index of the next field that will get considered. Excludes the last field of the current run length that is being
      // compared in this iteration
      final int nextField = fieldsComparedTillNow + runLengthEntry.getRunLength();
      final int currentRunEndPosition1 = RowKeyReader.fieldEndPosition(keyArray1, nextField - 1);
      final int currentRunEndPosition2 = RowKeyReader.fieldEndPosition(keyArray2, nextField - 1);

      final int cmp;

      if (!runLengthEntry.isByteComparable()) {
        // Only complex types are not byte comparable. Nested arrays aren't supported in MSQ
        assert runLengthEntry.getRunLength() == 1;
        cmp = FrameReaderUtils.compareComplexTypes(
            keyArray1,
            currentRunStartPosition1,
            keyArray2,
            currentRunStartPosition2,
            columnTypes[i],
            complexMetricSerdes[i]
        );
      } else {
        // The keys are byte comparable
        cmp = FrameReaderUtils.compareByteArraysUnsigned(
            keyArray1,
            currentRunStartPosition1,
            currentRunEndPosition1 - currentRunStartPosition1,
            keyArray2,
            currentRunStartPosition2,
            currentRunEndPosition2 - currentRunStartPosition2
        );
      }

      if (cmp != 0) {
        return runLengthEntry.getOrder() == KeyOrder.ASCENDING ? cmp : -cmp;
      }

      fieldsComparedTillNow = nextField;
      currentRunStartPosition1 = currentRunEndPosition1;
      currentRunStartPosition2 = currentRunEndPosition2;
    }
    return 0;
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
    ByteRowKeyComparator that = (ByteRowKeyComparator) o;
    return firstFieldPosition == that.firstFieldPosition
           && Objects.equals(keyColumns, that.keyColumns)
           && Objects.equals(rowKeyComparisonRunLengths, that.rowKeyComparisonRunLengths)
           && Objects.equals(rowSignature, that.rowSignature)
           && Arrays.equals(complexMetricSerdes, that.complexMetricSerdes)
           && Arrays.equals(columnTypes, that.columnTypes);
  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(keyColumns, firstFieldPosition, rowKeyComparisonRunLengths, rowSignature);
    result = 31 * result + Arrays.hashCode(complexMetricSerdes);
    result = 31 * result + Arrays.hashCode(columnTypes);
    return result;
  }

  @Override
  public String toString()
  {
    return "ByteRowKeyComparator{" +
           "keyColumns=" + keyColumns +
           ", firstFieldPosition=" + firstFieldPosition +
           ", rowKeyComparisonRunLengths=" + rowKeyComparisonRunLengths +
           ", rowSignature=" + rowSignature +
           ", complexMetricSerdes=" + Arrays.toString(complexMetricSerdes) +
           ", columnTypes=" + Arrays.toString(columnTypes) +
           '}';
  }
}
