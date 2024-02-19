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

import com.google.common.math.LongMath;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.write.columnar.NumericArrayFrameColumnWriter;
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
import java.util.Comparator;

public abstract class NumericArrayFrameColumnReader implements FrameColumnReader
{
  private final byte typeCode;
  private final ColumnType columnType;
  private final int columnNumber;

  public NumericArrayFrameColumnReader(byte typeCode, ColumnType columnType, int columnNumber)
  {
    this.typeCode = typeCode;
    this.columnType = columnType;
    this.columnNumber = columnNumber;
  }

  @Override
  public Column readRACColumn(Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    validate(memory);
    return new ColumnAccessorBasedColumn(column(frame, memory, columnType));
  }

  @Override
  public ColumnPlus readColumn(Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    validate(memory);
    return new ColumnPlus(
        column(frame, memory, columnType),
        ColumnCapabilitiesImpl.createSimpleArrayColumnCapabilities(columnType),
        frame.numRows()
    );
  }

  abstract NumericArrayFrameColumn column(Frame frame, Memory memory, ColumnType columnType);

  private void validate(final Memory region)
  {
    if (region.getCapacity() < NumericArrayFrameColumnWriter.DATA_OFFSET) {
      throw DruidException.defensive("Column[%s] is not big enough for a header", columnNumber);
    }
    final byte actualTypeCode = region.getByte(0);
    if (actualTypeCode != this.typeCode) {
      throw DruidException.defensive(
          "Column[%s] does not have the correct type code; expected[%s], got[%s]",
          columnNumber,
          this.typeCode,
          actualTypeCode
      );
    }
  }

  private static long getStartOfCumulativeLengthSection()
  {
    return NumericArrayFrameColumnWriter.DATA_OFFSET;
  }

  private static long getStartOfRowNullityData(final int numRows)
  {
    return getStartOfCumulativeLengthSection() + ((long) numRows * Integer.BYTES);
  }

  private static long getStartOfRowData(final Memory memory, final int numRows)
  {
    long nullityDataOffset =
        (long) Byte.BYTES * FrameColumnReaderUtils.getAdjustedCumulativeRowLength(
            memory,
            getStartOfCumulativeLengthSection(),
            numRows - 1
        );
    return LongMath.checkedAdd(getStartOfRowNullityData(numRows), nullityDataOffset);
  }

  public abstract static class NumericArrayFrameColumn extends ObjectColumnAccessorBase implements BaseColumn
  {

    private final Frame frame;
    private final Memory memory;
    private final ColumnType columnType;

    private final long rowNullityDataOffset;
    private final long rowDataOffset;


    public NumericArrayFrameColumn(Frame frame, Memory memory, ColumnType columnType)
    {
      this.frame = frame;
      this.memory = memory;
      this.columnType = columnType;

      this.rowNullityDataOffset = getStartOfRowNullityData(frame.numRows());
      this.rowDataOffset = getStartOfRowData(memory, frame.numRows());
    }

    @Override
    public ColumnType getType()
    {
      return columnType;
    }

    @Override
    public int numRows()
    {
      return frame.numRows();
    }

    @Override
    protected Object getVal(int rowNum)
    {
      return getNumericArray(physicalRow(rowNum));
    }

    @Override
    protected Comparator<Object> getComparator()
    {
      return columnType.getNullableStrategy();
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
    {
      return new ObjectColumnSelector<Object>()
      {
        private int cachedLogicalRow = -1;
        @Nullable
        private Object[] cachedValue = null;

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
        }

        @Nullable
        @Override
        public Object getObject()
        {
          compute();
          return cachedValue;
        }

        @Override
        public Class<?> classOfObject()
        {
          return Object[].class;
        }

        private void compute()
        {
          int currentLogicalRow = offset.getOffset();
          if (cachedLogicalRow == currentLogicalRow) {
            return;
          }
          cachedValue = getNumericArray(physicalRow(currentLogicalRow));
          cachedLogicalRow = currentLogicalRow;
        }
      };
    }

    @Override
    public VectorObjectSelector makeVectorObjectSelector(ReadableVectorOffset offset)
    {
      return new VectorObjectSelector()
      {
        private final Object[] vector = new Object[offset.getMaxVectorSize()];
        private int id = ReadableVectorInspector.NULL_ID;

        @Override
        public Object[] getObjectVector()
        {
          computeVector();
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

        private void computeVector()
        {
          if (id == offset.getId()) {
            return;
          }

          if (offset.isContiguous()) {
            // Contiguous offsets can have a cache optimized implementation if 'frame.isPermuted() == false',
            // i.e. logicalRow == physicalRow. The implementation can separately fetch out the nullity data, and the
            // element data continguously.
            final int start = offset.getStartOffset();
            for (int i = 0; i < offset.getCurrentVectorSize(); ++i) {
              vector[i] = getNumericArray(physicalRow(start + i));
            }
          } else {
            final int[] offsets = offset.getOffsets();
            for (int i = 0; i < offset.getCurrentVectorSize(); ++i) {
              vector[i] = getNumericArray(physicalRow(offsets[i]));
            }

            id = offset.getId();
          }
        }
      };
    }

    @Override
    public void close()
    {
      // Do nothing
    }

    private int physicalRow(int logicalRow)
    {
      return frame.physicalRow(logicalRow);
    }

    @Nullable
    private Object[] getNumericArray(final int physicalRow)
    {
      final int cumulativeLength = FrameColumnReaderUtils.getCumulativeRowLength(
          memory,
          getStartOfCumulativeLengthSection(),
          physicalRow
      );

      final int rowLength;
      if (FrameColumnReaderUtils.isNullRow(cumulativeLength)) {
        return null;
      } else if (physicalRow == 0) {
        rowLength = cumulativeLength;
      } else {
        final int previousCumulativeLength = FrameColumnReaderUtils.adjustCumulativeRowLength(
            FrameColumnReaderUtils.getCumulativeRowLength(
                memory,
                getStartOfCumulativeLengthSection(),
                physicalRow - 1
            )
        );
        rowLength = cumulativeLength - previousCumulativeLength;
      }

      if (rowLength == 0) {
        return ObjectArrays.EMPTY_ARRAY;
      }

      final Object[] row = new Object[rowLength];
      for (int i = 0; i < rowLength; ++i) {
        final int cumulativeIndex = cumulativeLength - rowLength + i;
        row[i] = getElementNullity(cumulativeIndex) ? null : getElement(memory, rowDataOffset, cumulativeIndex);
      }

      return row;
    }

    private boolean getElementNullity(final int cumulativeIndex)
    {
      byte b = memory.getByte(LongMath.checkedAdd(rowNullityDataOffset, (long) cumulativeIndex * Byte.BYTES));
      if (b == NumericArrayFrameColumnWriter.NULL_ELEMENT_MARKER) {
        return true;
      }
      assert b == NumericArrayFrameColumnWriter.NON_NULL_ELEMENT_MARKER;
      return false;
    }

    abstract Number getElement(Memory memory, long rowDataOffset, int cumulativeIndex);
  }
}
