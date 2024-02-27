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

import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.field.FieldReader;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.frame.write.RowBasedFrameWriter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;

/**
 * Implementation of {@link FrameComparisonWidget} for pairs of {@link FrameType#ROW_BASED} frames.
 *
 * Comparison logic in this class is very similar to {@link RowKeyComparator}, but is different because it works
 * on Frames instead of byte[].
 */
public class FrameComparisonWidgetImpl implements FrameComparisonWidget
{
  private final Frame frame;
  private final RowSignature signature;
  private final Memory rowOffsetRegion;
  private final Memory dataRegion;
  private final int keyFieldCount;
  private final List<FieldReader> keyFieldReaders;
  private final int firstFieldPosition;
  private final int[] ascDescRunLengths;

  private FrameComparisonWidgetImpl(
      final Frame frame,
      final RowSignature signature,
      final Memory rowOffsetRegion,
      final Memory dataRegion,
      final List<FieldReader> keyFieldReaders,
      final int firstFieldPosition,
      final int[] ascDescRunLengths
  )
  {
    this.frame = frame;
    this.signature = signature;
    this.rowOffsetRegion = rowOffsetRegion;
    this.dataRegion = dataRegion;
    this.keyFieldCount = keyFieldReaders.size();
    this.keyFieldReaders = keyFieldReaders;
    this.firstFieldPosition = firstFieldPosition;
    this.ascDescRunLengths = ascDescRunLengths;
  }

  /**
   * Create a {@link FrameComparisonWidget} for the given frame.
   *
   * Only possible for frames of type {@link FrameType#ROW_BASED}. The provided keyColumns must be a prefix
   * of {@link FrameReader#signature()}.
   *
   * @param frame            frame, must be {@link FrameType#ROW_BASED}
   * @param signature        signature for the frame
   * @param keyColumns       columns to sort by
   * @param keyColumnReaders readers for key columns
   */
  public static FrameComparisonWidgetImpl create(
      final Frame frame,
      final RowSignature signature,
      final List<KeyColumn> keyColumns,
      final List<FieldReader> keyColumnReaders
  )
  {
    FrameWriterUtils.verifySortColumns(keyColumns, signature);

    if (keyColumnReaders.size() != keyColumns.size()) {
      throw new ISE("Mismatched lengths for keyColumnReaders and keyColumns");
    }

    return new FrameComparisonWidgetImpl(
        FrameType.ROW_BASED.ensureType(frame),
        signature,
        frame.region(RowBasedFrameWriter.ROW_OFFSET_REGION),
        frame.region(RowBasedFrameWriter.ROW_DATA_REGION),
        keyColumnReaders,
        ByteRowKeyComparator.computeFirstFieldPosition(signature.size()),
        ByteRowKeyComparator.computeAscDescRunLengths(keyColumns)
    );
  }

  @Override
  public RowKey readKey(int row)
  {
    if (keyFieldCount == 0) {
      return RowKey.empty();
    }

    final int keyFieldPointersEndInRow = keyFieldCount * Integer.BYTES;
    final long rowPosition = getRowPositionInDataRegion(row);
    final int keyEndInRow =
        dataRegion.getInt(rowPosition + (long) (keyFieldCount - 1) * Integer.BYTES);

    final long keyLength = keyEndInRow - firstFieldPosition;
    final byte[] keyBytes = new byte[Ints.checkedCast(keyFieldPointersEndInRow + keyEndInRow - firstFieldPosition)];

    final int headerSizeAdjustment = (signature.size() - keyFieldCount) * Integer.BYTES;
    for (int i = 0; i < keyFieldCount; i++) {
      final int fieldEndPosition = dataRegion.getInt(rowPosition + ((long) Integer.BYTES * i));
      final int adjustedFieldEndPosition = fieldEndPosition - headerSizeAdjustment;
      keyBytes[Integer.BYTES * i] = (byte) adjustedFieldEndPosition;
      keyBytes[Integer.BYTES * i + 1] = (byte) (adjustedFieldEndPosition >> 8);
      keyBytes[Integer.BYTES * i + 2] = (byte) (adjustedFieldEndPosition >> 16);
      keyBytes[Integer.BYTES * i + 3] = (byte) (adjustedFieldEndPosition >> 24);
    }

    for (int i = 0; i < keyLength; i++) {
      keyBytes[keyFieldPointersEndInRow + i] = dataRegion.getByte(rowPosition + firstFieldPosition + i);
    }

    return RowKey.wrap(keyBytes);
  }

  @Override
  public boolean isCompletelyNonNullKey(int row)
  {
    if (keyFieldCount == 0) {
      return true;
    }

    final long rowPosition = getRowPositionInDataRegion(row);
    long keyFieldPosition = rowPosition + (long) signature.size() * Integer.BYTES;

    for (int i = 0; i < keyFieldCount; i++) {
      final boolean isNull = keyFieldReaders.get(i).isNull(dataRegion, keyFieldPosition);
      if (isNull) {
        return false;
      } else {
        keyFieldPosition = rowPosition + dataRegion.getInt(rowPosition + (long) i * Integer.BYTES);
      }
    }

    return true;
  }

  @Override
  public int compare(int row, RowKey key)
  {
    // Similar logic to RowKeyComparator, but implementation is different enough that we need our own.
    // Major difference is Frame v. byte[] instead of byte[] v. byte[].

    final byte[] keyArray = key.array();
    final long rowPosition = getRowPositionInDataRegion(row);

    long comparableBytesStartPositionInRow = firstFieldPosition;
    int keyComparableBytesStartPosition = Integer.BYTES * keyFieldCount;

    boolean ascending = true;
    int field = 0;

    for (int numFields : ascDescRunLengths) {
      if (numFields > 0) {
        final int nextField = field + numFields;
        final long comparableBytesEndPositionInRow = getFieldEndPositionInRow(rowPosition, nextField - 1);
        final int keyComparableBytesEndPosition = RowKeyReader.fieldEndPosition(keyArray, nextField - 1);

        final long comparableBytesLength = comparableBytesEndPositionInRow - comparableBytesStartPositionInRow;
        final int keyComparableBytesLength = keyComparableBytesEndPosition - keyComparableBytesStartPosition;

        int cmp = FrameReaderUtils.compareMemoryToByteArrayUnsigned(
            dataRegion,
            rowPosition + comparableBytesStartPositionInRow,
            comparableBytesLength,
            keyArray,
            keyComparableBytesStartPosition,
            keyComparableBytesLength
        );

        if (cmp != 0) {
          return ascending ? cmp : -cmp;
        }

        field += numFields;
        comparableBytesStartPositionInRow += comparableBytesLength;
        keyComparableBytesStartPosition += keyComparableBytesLength;
      }

      ascending = !ascending;
    }

    return 0;
  }

  @Override
  public int compare(final int row, final FrameComparisonWidget otherWidget, final int otherRow)
  {
    // Similar logic to RowKeyComparator, but implementation is different enough that we need our own.
    // Major difference is Frame v. Frame instead of byte[] v. byte[].

    final FrameComparisonWidgetImpl otherWidgetImpl = (FrameComparisonWidgetImpl) otherWidget;

    final long rowPosition = getRowPositionInDataRegion(row);
    final long otherRowPosition = otherWidgetImpl.getRowPositionInDataRegion(otherRow);

    int comparableBytesStartPositionInRow = firstFieldPosition;
    int otherComparableBytesStartPositionInRow = otherWidgetImpl.firstFieldPosition;

    boolean ascending = true;
    int field = 0;

    for (int numFields : ascDescRunLengths) {
      if (numFields > 0) {
        final int nextField = field + numFields;
        final int comparableBytesEndPositionInRow = getFieldEndPositionInRow(rowPosition, nextField - 1);
        final int otherComparableBytesEndPositionInRow =
            otherWidgetImpl.getFieldEndPositionInRow(otherRowPosition, nextField - 1);

        final int comparableBytesLength = comparableBytesEndPositionInRow - comparableBytesStartPositionInRow;
        final int otherComparableBytesLength =
            otherComparableBytesEndPositionInRow - otherComparableBytesStartPositionInRow;

        int cmp = FrameReaderUtils.compareMemoryUnsigned(
            dataRegion,
            rowPosition + comparableBytesStartPositionInRow,
            comparableBytesLength,
            otherWidgetImpl.getDataRegion(),
            otherRowPosition + otherComparableBytesStartPositionInRow,
            otherComparableBytesLength
        );

        if (cmp != 0) {
          return ascending ? cmp : -cmp;
        }

        field += numFields;
        comparableBytesStartPositionInRow += comparableBytesLength;
        otherComparableBytesStartPositionInRow += otherComparableBytesLength;
      }

      ascending = !ascending;
    }

    return 0;
  }

  long getRowPositionInDataRegion(final int logicalRow)
  {
    final int physicalRowNumber = frame.physicalRow(logicalRow);

    if (physicalRowNumber == 0) {
      return 0;
    } else {
      return rowOffsetRegion.getLong((long) Long.BYTES * (physicalRowNumber - 1));
    }
  }

  int getFieldEndPositionInRow(final long rowPosition, final int fieldNumber)
  {
    assert fieldNumber >= 0 && fieldNumber < signature.size();
    return dataRegion.getInt(rowPosition + (long) fieldNumber * Integer.BYTES);
  }

  Memory getDataRegion()
  {
    return dataRegion;
  }
}
