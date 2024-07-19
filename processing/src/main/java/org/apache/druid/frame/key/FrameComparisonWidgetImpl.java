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
import org.apache.datasketches.memory.Memory;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.field.FieldReader;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.frame.write.RowBasedFrameWriter;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;

/**
 * Implementation of {@link FrameComparisonWidget} for pairs of {@link FrameType#ROW_BASED} frames.
 *
 * Comparison logic in this class is very similar to {@link RowKeyComparator}, but is different because it works
 * on Frames instead of byte[].
 */
@NotThreadSafe
public class FrameComparisonWidgetImpl implements FrameComparisonWidget
{

  /**
   * Frame for comparison
   */
  private final Frame frame;

  /**
   * Row signature of the frame
   */
  private final RowSignature signature;

  /**
   * Row offset region of the frame
   */
  private final Memory rowOffsetRegion;

  /**
   * Data region of the frame
   */
  private final Memory dataRegion;

  /**
   * Number of fields in the key column
   */
  private final int keyFieldCount;

  /**
   * Field readers for the key
   */
  private final List<FieldReader> keyFieldReaders;

  /**
   * Starting position of the first field in the row
   */
  private final int firstFieldPosition;

  /**
   * Run lengths created for comparing the key columns
   */
  private final RowKeyComparisonRunLengths rowKeyComparisonRunLengths;

  /**
   * Pre-computed array of ComplexMetricSerde corresponding to the computed run-lengths. If the run length entry is
   * byte-comparable, the corresponding serde is null, and if it's not byte comparable, the corresponding serde isn't null
   * (since only complex columns are not byte comparable)
   */
  private final ComplexMetricSerde[] complexMetricSerdes;
  private final ColumnType[] columnTypes;

  private FrameComparisonWidgetImpl(
      final Frame frame,
      final RowSignature signature,
      final Memory rowOffsetRegion,
      final Memory dataRegion,
      final List<FieldReader> keyFieldReaders,
      final int firstFieldPosition,
      final RowKeyComparisonRunLengths rowKeyComparisonRunLengths,
      final ComplexMetricSerde[] complexMetricSerdes,
      final ColumnType[] columnTypes
  )
  {
    this.frame = frame;
    this.signature = signature;
    this.rowOffsetRegion = rowOffsetRegion;
    this.dataRegion = dataRegion;
    this.keyFieldCount = keyFieldReaders.size();
    this.keyFieldReaders = keyFieldReaders;
    this.firstFieldPosition = firstFieldPosition;
    this.rowKeyComparisonRunLengths = rowKeyComparisonRunLengths;
    this.complexMetricSerdes = complexMetricSerdes;
    this.columnTypes = columnTypes;
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

    final RowKeyComparisonRunLengths rowKeyComparisonRunLengths = RowKeyComparisonRunLengths.create(keyColumns, signature);
    final RunLengthEntry[] runLengthEntries = rowKeyComparisonRunLengths.getRunLengthEntries();
    final ComplexMetricSerde[] complexMetricSerdes = new ComplexMetricSerde[runLengthEntries.length];
    final ColumnType[] columnTypes = new ColumnType[runLengthEntries.length];

    int fieldsSeenSoFar = 0;
    for (int i = 0; i < runLengthEntries.length; ++i) {
      // If the run length entry isn't byte comparable, it most definitely is a complex type
      if (runLengthEntries[i].isByteComparable()) {
        complexMetricSerdes[i] = null;
        columnTypes[i] = null;
      } else {
        final String columnName = keyColumns.get(fieldsSeenSoFar).columnName();
        final ColumnType columnType = signature.getColumnType(columnName).orElse(null);
        if (columnType == null) {
          throw DruidException.defensive(
              "Cannot compare on the byte incomparable column [%s] without knowing it's type",
              columnName
          );
        }
        final String complexTypeName = columnType.getComplexTypeName();
        if (complexTypeName == null) {
          throw DruidException.defensive("Expected complex type name for column [%s] for comparison", columnName);
        }
        complexMetricSerdes[i] = Preconditions.checkNotNull(
            ComplexMetrics.getSerdeForType(complexTypeName),
            "Cannot find serde for column [%s] of type [%s]",
            columnName,
            complexTypeName
        );
        columnTypes[i] = columnType;
      }
      fieldsSeenSoFar += runLengthEntries[i].getRunLength();
    }

    return new FrameComparisonWidgetImpl(
        FrameType.ROW_BASED.ensureType(frame),
        signature,
        frame.region(RowBasedFrameWriter.ROW_OFFSET_REGION),
        frame.region(RowBasedFrameWriter.ROW_DATA_REGION),
        keyColumnReaders,
        ByteRowKeyComparator.computeFirstFieldPosition(signature.size()),
        rowKeyComparisonRunLengths,
        complexMetricSerdes,
        columnTypes
    );
  }

  /**
   * Creates {@link RowKey} from a row in the frame. See the layout of the {@link RowKey}
   */
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

    // Length of the portion of the header which isn't included in the rowKey
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
  public boolean hasNonNullKeyParts(int row, int[] keyParts)
  {
    if (keyParts.length == 0) {
      return true;
    }

    final long rowPosition = getRowPositionInDataRegion(row);

    for (int i : keyParts) {
      if (i < 0 || i >= keyFieldCount) {
        throw new IAE("Invalid key part[%d]", i);
      }

      final long keyFieldPosition;

      if (i == 0) {
        keyFieldPosition = rowPosition + (long) signature.size() * Integer.BYTES;
      } else {
        keyFieldPosition = rowPosition + dataRegion.getInt(rowPosition + (long) (i - 1) * Integer.BYTES);
      }

      if (keyFieldReaders.get(i).isNull(dataRegion, keyFieldPosition)) {
        return false;
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
    int comparableBytesStartPositionInKey = Integer.BYTES * keyFieldCount;

    // Number of fields compared till now, which is equivalent to the index of the field to compare next
    int fieldsComparedTillNow = 0;

    for (int i = 0; i < rowKeyComparisonRunLengths.getRunLengthEntries().length; ++i) {

      final RunLengthEntry runLengthEntry = rowKeyComparisonRunLengths.getRunLengthEntries()[i];

      if (runLengthEntry.getRunLength() <= 0) {
        // Defensive check
        continue;
      }

      // Index of the next field that will get considered. Excludes the current field that we are comparing right now
      final int nextField = fieldsComparedTillNow + runLengthEntry.getRunLength();
      final int comparableBytesEndPositionInKey = RowKeyReader.fieldEndPosition(keyArray, nextField - 1);
      final int comparableBytesEndPositionInRow = getFieldEndPositionInRow(rowPosition, nextField - 1);

      final int cmp;

      if (!runLengthEntry.isByteComparable()) {
        // Only complex types are not byte comparable. Nested arrays aren't supported in MSQ
        assert runLengthEntry.getRunLength() == 1;
        cmp = FrameReaderUtils.compareComplexTypes(
            dataRegion,
            rowPosition + comparableBytesStartPositionInRow,
            keyArray,
            comparableBytesStartPositionInKey,
            columnTypes[i],
            complexMetricSerdes[i]
        );
      } else {
        cmp = FrameReaderUtils.compareMemoryToByteArrayUnsigned(
            dataRegion,
            rowPosition + comparableBytesStartPositionInRow,
            comparableBytesEndPositionInRow - comparableBytesStartPositionInRow,
            keyArray,
            comparableBytesStartPositionInKey,
            comparableBytesEndPositionInKey - comparableBytesStartPositionInKey
        );
      }

      if (cmp != 0) {
        return runLengthEntry.getOrder() == KeyOrder.ASCENDING ? cmp : -cmp;
      }
      fieldsComparedTillNow = nextField;
      comparableBytesStartPositionInRow = comparableBytesEndPositionInRow;
      comparableBytesStartPositionInKey = comparableBytesEndPositionInKey;
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

    // Number of fields compared till now, which is equivalent to the index of the field to compare next
    int fieldsComparedTillNow = 0;

    for (int i = 0; i < rowKeyComparisonRunLengths.getRunLengthEntries().length; ++i) {
      final RunLengthEntry runLengthEntry = rowKeyComparisonRunLengths.getRunLengthEntries()[i];
      if (runLengthEntry.getRunLength() <= 0) {
        // Defensive check
        continue;
      }

      final int nextField = fieldsComparedTillNow + runLengthEntry.getRunLength();
      final int comparableBytesEndPositionInRow = getFieldEndPositionInRow(rowPosition, nextField - 1);
      final int otherComparableBytesEndPositionInRow = otherWidgetImpl.getFieldEndPositionInRow(otherRowPosition, nextField - 1);

      final int cmp;

      if (!runLengthEntry.isByteComparable()) {
        // Only complex types are not byte comparable. Nested arrays aren't supported in MSQ
        assert runLengthEntry.getRunLength() == 1;

        cmp = FrameReaderUtils.compareComplexTypes(
            dataRegion,
            rowPosition + comparableBytesStartPositionInRow,
            otherWidgetImpl.dataRegion,
            otherRowPosition + otherComparableBytesStartPositionInRow,
            columnTypes[i],
            complexMetricSerdes[i]
        );
      } else {
        cmp = FrameReaderUtils.compareMemoryUnsigned(
            dataRegion,
            rowPosition + comparableBytesStartPositionInRow,
            comparableBytesEndPositionInRow - comparableBytesStartPositionInRow,
            otherWidgetImpl.getDataRegion(),
            otherRowPosition + otherComparableBytesStartPositionInRow,
            otherComparableBytesEndPositionInRow - otherComparableBytesStartPositionInRow
        );
      }

      if (cmp != 0) {
        return runLengthEntry.getOrder() == KeyOrder.ASCENDING ? cmp : -cmp;
      }

      fieldsComparedTillNow = nextField;
      comparableBytesStartPositionInRow = comparableBytesEndPositionInRow;
      otherComparableBytesStartPositionInRow = otherComparableBytesEndPositionInRow;
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
