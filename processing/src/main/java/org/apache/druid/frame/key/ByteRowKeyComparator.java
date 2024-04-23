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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.data.input.Row;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ValueType;
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

  private ByteRowKeyComparator(
      final List<KeyColumn> keyColumns,
      final RowKeyComparisonRunLengths rowKeyComparisonRunLengths
  )
  {
    this.keyColumns = keyColumns;
    this.firstFieldPosition = computeFirstFieldPosition(keyColumns.size());
    this.rowKeyComparisonRunLengths = rowKeyComparisonRunLengths;
  }

  public static ByteRowKeyComparator create(final List<KeyColumn> keyColumns)
  {
    return new ByteRowKeyComparator(
        keyColumns,
        RowKeyComparisonRunLengths.create(keyColumns)
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

    for (RowKeyComparisonRunLengths.RunLengthEntry runLengthEntry : rowKeyComparisonRunLengths.getRunLengthEntries()) {

      if (runLengthEntry.getRunLength() <= 0) {
        // Defensive check
        continue;
      }

      if (!runLengthEntry.isByteComparable()) {
        // Only complex types are not byte comparable. Nested arrays aren't supported in MSQ
        assert runLengthEntry.getRunLength() == 1;
        // 'fieldsComparedTillNow' is the index of the current keyColumn in the keyColumns list. Sanity check that its
        // a known complex type
        String complexTypeName = Preconditions.checkNotNull(
            keyColumns.get(fieldsComparedTillNow).columnType().getComplexTypeName(),
            "complexType must be present for comparison"
        );

        ComplexMetricSerde serde = Preconditions.checkNotNull(
            ComplexMetrics.getSerdeForType(complexTypeName),
            "serde for type [%s] not present",
            complexTypeName
        );

        // Index of the next field that will get considered. Excludes the current field that we are comparing right now
        final int nextField = fieldsComparedTillNow + 1;
        final int currentRunEndPosition1 = RowKeyReader.fieldEndPosition(keyArray1, nextField - 1);
        final int currentRunEndPosition2 = RowKeyReader.fieldEndPosition(keyArray2, nextField - 1);

        int cmp = FrameReaderUtils.compareComplexTypes(
            keyArray1,
            currentRunStartPosition1,
            currentRunEndPosition1 - currentRunStartPosition1,
            keyArray2,
            currentRunStartPosition2,
            currentRunEndPosition2 - currentRunStartPosition2,
            keyColumns.get(fieldsComparedTillNow).columnType(),
            serde
        );
        if (cmp != 0) {
          return runLengthEntry.getOrder() == KeyOrder.ASCENDING ? cmp : -cmp;
        }
        // We have only compared a single field here
        fieldsComparedTillNow = nextField;
        currentRunStartPosition1 = currentRunEndPosition1;
        currentRunStartPosition2 = currentRunEndPosition2;
      } else {
        // The keys are byte comparable
        int nextField = fieldsComparedTillNow + runLengthEntry.getRunLength();
        final int currentRunEndPosition1 = RowKeyReader.fieldEndPosition(keyArray1, nextField - 1);
        final int currentRunEndPosition2 = RowKeyReader.fieldEndPosition(keyArray2, nextField - 1);
        int cmp = FrameReaderUtils.compareByteArraysUnsigned(
            keyArray1,
            currentRunStartPosition1,
            currentRunEndPosition1 - currentRunStartPosition1,
            keyArray2,
            currentRunStartPosition2,
            currentRunEndPosition2 - currentRunStartPosition2
        );
        if (cmp != 0) {
          return runLengthEntry.getOrder() == KeyOrder.ASCENDING ? cmp : -cmp;
        }

        fieldsComparedTillNow = nextField;
        currentRunStartPosition1 = currentRunEndPosition1;
        currentRunStartPosition2 = currentRunEndPosition2;
      }
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
           && Objects.equals(rowKeyComparisonRunLengths, that.rowKeyComparisonRunLengths);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(keyColumns, firstFieldPosition, rowKeyComparisonRunLengths);
  }

  @Override
  public String toString()
  {
    return "ByteRowKeyComparator{" +
           "keyColumns=" + keyColumns +
           ", firstFieldPosition=" + firstFieldPosition +
           ", rowKeyComparisonRunLengths=" + rowKeyComparisonRunLengths +
           '}';
  }
}
