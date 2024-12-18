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

package org.apache.druid.frame.read.columnar;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.frame.write.columnar.FrameColumnWriters;
import org.apache.druid.frame.write.columnar.StringFrameColumnWriter;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;
import org.apache.druid.query.rowsandcols.column.accessor.ObjectColumnAccessorBase;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Reader for {@link ColumnType#STRING_ARRAY}.
 * This is similar to {@link StringFrameColumnReader} reading mvds in reading bytes from frame
 */
public class StringArrayFrameColumnReader implements FrameColumnReader
{
  private final int columnNumber;

  /**
   * Create a new reader.
   *
   * @param columnNumber column number
   */
  StringArrayFrameColumnReader(int columnNumber)
  {
    this.columnNumber = columnNumber;
  }

  @Override
  public Column readRACColumn(Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    validate(memory);

    final long positionOfLengths = getStartOfStringLengthSection(frame.numRows());
    final long positionOfPayloads = getStartOfStringDataSection(memory, frame.numRows());

    StringArrayFrameColumn frameCol = new StringArrayFrameColumn(
        frame,
        memory,
        positionOfLengths,
        positionOfPayloads
    );

    return new ColumnAccessorBasedColumn(frameCol);
  }

  @Override
  public ColumnPlus readColumn(final Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    validate(memory);

    final long startOfStringLengthSection = getStartOfStringLengthSection(frame.numRows());
    final long startOfStringDataSection = getStartOfStringDataSection(memory, frame.numRows());

    final BaseColumn baseColumn = new StringArrayFrameColumn(
        frame,
        memory,
        startOfStringLengthSection,
        startOfStringDataSection
    );

    return new ColumnPlus(
        baseColumn,
        new ColumnCapabilitiesImpl().setType(ColumnType.STRING_ARRAY)
                                    .setHasMultipleValues(false)
                                    .setDictionaryEncoded(false),
        frame.numRows()
    );
  }

  private void validate(final Memory region)
  {
    // Check if column is big enough for a header
    if (region.getCapacity() < StringFrameColumnWriter.DATA_OFFSET) {
      throw DruidException.defensive("Column[%s] is not big enough for a header", columnNumber);
    }

    final byte typeCode = region.getByte(0);
    if (typeCode != FrameColumnWriters.TYPE_STRING_ARRAY) {
      throw DruidException.defensive(
          "Column[%s] does not have the correct type code; expected[%s], got[%s]",
          columnNumber,
          FrameColumnWriters.TYPE_STRING_ARRAY,
          typeCode
      );
    }
  }

  private static long getStartOfCumulativeLengthSection()
  {
    return StringFrameColumnWriter.DATA_OFFSET;
  }

  private static long getStartOfStringLengthSection(final int numRows)
  {
    return StringFrameColumnWriter.DATA_OFFSET + (long) Integer.BYTES * numRows;
  }

  private long getStartOfStringDataSection(
      final Memory memory,
      final int numRows
  )
  {
    if (numRows < 0) {
      throw DruidException.defensive("Encountered -ve numRows [%s] while reading frame", numRows);
    }
    final int totalNumValues = FrameColumnReaderUtils.getAdjustedCumulativeRowLength(
        memory,
        getStartOfCumulativeLengthSection(),
        numRows - 1
    );

    return getStartOfStringLengthSection(numRows) + (long) Integer.BYTES * totalNumValues;
  }

  private static class StringArrayFrameColumn extends ObjectColumnAccessorBase implements BaseColumn
  {
    private final Frame frame;
    private final Memory memory;
    private final long startOfStringLengthSection;
    private final long startOfStringDataSection;

    private StringArrayFrameColumn(
        Frame frame,
        Memory memory,
        long startOfStringLengthSection,
        long startOfStringDataSection
    )
    {
      this.frame = frame;
      this.memory = memory;
      this.startOfStringLengthSection = startOfStringLengthSection;
      this.startOfStringDataSection = startOfStringDataSection;
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
    {
      return new ObjectColumnSelector<>()
      {
        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // Do nothing.
        }

        @Nullable
        @Override
        public Object getObject()
        {
          return getRowAsObject(frame.physicalRow(offset.getOffset()), true);
        }

        @Override
        public Class<?> classOfObject()
        {
          return Object[].class;
        }
      };
    }

    @Override
    public VectorObjectSelector makeVectorObjectSelector(final ReadableVectorOffset offset)
    {
      class StringArrayFrameVectorObjectSelector implements VectorObjectSelector
      {
        private final Object[] vector = new Object[offset.getMaxVectorSize()];
        private int id = ReadableVectorInspector.NULL_ID;

        @Override
        public Object[] getObjectVector()
        {
          computeVectorIfNeeded();
          return vector;
        }

        @Override
        public int getMaxVectorSize()
        {
          return offset.getMaxVectorSize();
        }

        @Override
        public int getCurrentVectorSize()
        {
          return offset.getCurrentVectorSize();
        }

        private void computeVectorIfNeeded()
        {
          if (id == offset.getId()) {
            return;
          }

          if (offset.isContiguous()) {
            final int start = offset.getStartOffset();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(i + start);
              vector[i] = getRowAsObject(physicalRow, true);
            }
          } else {
            final int[] offsets = offset.getOffsets();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(offsets[i]);
              vector[i] = getRowAsObject(physicalRow, true);
            }
          }

          id = offset.getId();
        }
      }

      return new StringArrayFrameVectorObjectSelector();
    }

    @Override
    public void close()
    {
      // Do nothing.
    }

    @Override
    public ColumnType getType()
    {
      return ColumnType.STRING_ARRAY;
    }

    @Override
    public int numRows()
    {
      return frame.numRows();
    }

    @Override
    protected Object getVal(int rowNum)
    {
      return getRowAsObject(frame.physicalRow(rowNum), true);
    }

    @Override
    protected Comparator<Object> getComparator()
    {
      return Comparator.nullsFirst(ColumnType.STRING_ARRAY.getStrategy());
    }

    /**
     * Returns a ByteBuffer containing UTF-8 encoded string number {@code index}. The ByteBuffer is always newly
     * created, so it is OK to change its position, limit, etc. However, it may point to shared memory, so it is
     * not OK to write to its contents.
     */
    @Nullable
    private ByteBuffer getStringUtf8(final int index)
    {
      if (startOfStringLengthSection > Long.MAX_VALUE - (long) Integer.BYTES * index) {
        throw DruidException.defensive("length index would overflow trying to read the frame memory!");
      }

      final int dataEndVariableIndex = memory.getInt(startOfStringLengthSection + (long) Integer.BYTES * index);
      if (startOfStringDataSection > Long.MAX_VALUE - dataEndVariableIndex) {
        throw DruidException.defensive("data end index would overflow trying to read the frame memory!");
      }

      final long dataStart;
      final long dataEnd = startOfStringDataSection + dataEndVariableIndex;

      if (index == 0) {
        dataStart = startOfStringDataSection;
      } else {
        final int dataStartVariableIndex = memory.getInt(startOfStringLengthSection + (long) Integer.BYTES * (index
                                                                                                              - 1));
        if (startOfStringDataSection > Long.MAX_VALUE - dataStartVariableIndex) {
          throw DruidException.defensive("data start index would overflow trying to read the frame memory!");
        }
        dataStart = startOfStringDataSection + dataStartVariableIndex;
      }

      final int dataLength = Ints.checkedCast(dataEnd - dataStart);

      if ((dataLength == 0 && NullHandling.replaceWithDefault()) ||
          (dataLength == 1 && memory.getByte(dataStart) == FrameWriterUtils.NULL_STRING_MARKER)) {
        return null;
      }

      return FrameReaderUtils.readByteBuffer(memory, dataStart, dataLength);
    }

    @Nullable
    private String getString(final int index)
    {
      final ByteBuffer stringUtf8 = getStringUtf8(index);

      if (stringUtf8 == null) {
        return null;
      } else {
        return StringUtils.fromUtf8(stringUtf8);
      }
    }

    /**
     * Returns the object at the given physical row number.
     *
     * @param physicalRow physical row number
     * @param decode      if true, return java.lang.String. If false, return UTF-8 ByteBuffer.
     */
    @Nullable
    private Object getRowAsObject(final int physicalRow, final boolean decode)
    {
      final int cumulativeRowLength = FrameColumnReaderUtils.getCumulativeRowLength(
          memory,
          getStartOfCumulativeLengthSection(),
          physicalRow
      );
      final int rowLength;

      if (FrameColumnReaderUtils.isNullRow(cumulativeRowLength)) {
        return null;
      } else if (physicalRow == 0) {
        rowLength = cumulativeRowLength;
      } else {
        rowLength = cumulativeRowLength - FrameColumnReaderUtils.getAdjustedCumulativeRowLength(
            memory,
            getStartOfCumulativeLengthSection(),
            physicalRow - 1
        );
      }

      if (rowLength == 0) {
        return ObjectArrays.EMPTY_ARRAY;
      } else {
        final Object[] row = new Object[rowLength];

        for (int i = 0; i < rowLength; i++) {
          final int index = cumulativeRowLength - rowLength + i;
          row[i] = decode ? getString(index) : getStringUtf8(index);
        }

        return row;
      }
    }
  }
}
