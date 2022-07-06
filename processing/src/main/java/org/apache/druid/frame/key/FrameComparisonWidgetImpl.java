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
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.frame.write.RowBasedFrameWriter;

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
  private final FrameReader frameReader;
  private final Memory rowOffsetRegion;
  private final Memory dataRegion;
  private final int keyFieldCount;
  private final long firstFieldPosition;
  private final int[] ascDescRunLengths;

  private FrameComparisonWidgetImpl(
      final Frame frame,
      final FrameReader frameReader,
      final Memory rowOffsetRegion,
      final Memory dataRegion,
      final int keyFieldCount,
      final long firstFieldPosition,
      final int[] ascDescRunLengths
  )
  {
    this.frame = frame;
    this.frameReader = frameReader;
    this.rowOffsetRegion = rowOffsetRegion;
    this.dataRegion = dataRegion;
    this.keyFieldCount = keyFieldCount;
    this.firstFieldPosition = firstFieldPosition;
    this.ascDescRunLengths = ascDescRunLengths;
  }

  /**
   * Create a {@link FrameComparisonWidget} for the given frame.
   *
   * Only possible for frames of type {@link FrameType#ROW_BASED}. The provided sortColumns must be a prefix
   * of {@link FrameReader#signature()}.
   *
   * @param frame       frame, must be {@link FrameType#ROW_BASED}
   * @param frameReader reader for the frame
   * @param sortColumns columns to sort by
   */
  public static FrameComparisonWidgetImpl create(
      final Frame frame,
      final FrameReader frameReader,
      final List<SortColumn> sortColumns
  )
  {
    FrameWriterUtils.verifySortColumns(sortColumns, frameReader.signature());

    return new FrameComparisonWidgetImpl(
        FrameType.ROW_BASED.ensureType(frame),
        frameReader,
        frame.region(RowBasedFrameWriter.ROW_OFFSET_REGION),
        frame.region(RowBasedFrameWriter.ROW_DATA_REGION),
        sortColumns.size(),
        RowKeyComparator.computeFirstFieldPosition(frameReader.signature().size()),
        RowKeyComparator.computeAscDescRunLengths(sortColumns)
    );
  }

  @Override
  public RowKey readKey(int row)
  {
    final int keyFieldPointersEndInRow = keyFieldCount * Integer.BYTES;

    if (keyFieldCount == 0) {
      return RowKey.empty();
    }

    final long rowPosition = getRowPositionInDataRegion(row);
    final int keyEndInRow =
        dataRegion.getInt(rowPosition + (long) (keyFieldCount - 1) * Integer.BYTES);

    final long keyLength = keyEndInRow - firstFieldPosition;
    final byte[] keyBytes = new byte[Ints.checkedCast(keyFieldPointersEndInRow + keyEndInRow - firstFieldPosition)];

    final int headerSizeAdjustment = (frameReader.signature().size() - keyFieldCount) * Integer.BYTES;
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

    long comparableBytesStartPositionInRow = firstFieldPosition;
    long otherComparableBytesStartPositionInRow = firstFieldPosition;

    boolean ascending = true;
    int field = 0;

    for (int numFields : ascDescRunLengths) {
      if (numFields > 0) {
        final int nextField = field + numFields;
        final long comparableBytesEndPositionInRow = getFieldEndPositionInRow(rowPosition, nextField - 1);
        final long otherComparableBytesEndPositionInRow =
            otherWidgetImpl.getFieldEndPositionInRow(otherRowPosition, nextField - 1);

        final long comparableBytesLength = comparableBytesEndPositionInRow - comparableBytesStartPositionInRow;
        final long otherComparableBytesLength =
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

  long getFieldEndPositionInRow(final long rowPosition, final int fieldNumber)
  {
    assert fieldNumber >= 0 && fieldNumber < frameReader.signature().size();

    return dataRegion.getInt(rowPosition + (long) fieldNumber * Integer.BYTES);
  }

  Memory getDataRegion()
  {
    return dataRegion;
  }
}
