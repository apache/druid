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

import org.apache.datasketches.memory.Memory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.write.columnar.FrameColumnWriters;
import org.apache.druid.frame.write.columnar.LongFrameColumnWriter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

public class LongFrameColumnReader implements FrameColumnReader
{
  private final int columnNumber;

  LongFrameColumnReader(final int columnNumber)
  {
    this.columnNumber = columnNumber;
  }

  @Override
  public ColumnPlus readColumn(final Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    validate(memory, frame.numRows());

    final boolean hasNulls = getHasNulls(memory);

    return new ColumnPlus(
        new LongFrameColumn(frame, hasNulls, memory),
        ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG)
                              .setHasNulls(NullHandling.sqlCompatible() && hasNulls),
        frame.numRows()
    );
  }

  private void validate(final Memory region, final int numRows)
  {
    // Check if column is big enough for a header
    if (region.getCapacity() < LongFrameColumnWriter.DATA_OFFSET) {
      throw new ISE("Column is not big enough for a header");
    }

    final byte typeCode = region.getByte(0);
    if (typeCode != FrameColumnWriters.TYPE_LONG) {
      throw new ISE("Column does not have the correct type code");
    }

    final boolean hasNulls = getHasNulls(region);
    final int sz = LongFrameColumnWriter.valueSize(hasNulls);

    // Check column length again, now that we know exactly how long it should be.
    if (region.getCapacity() != LongFrameColumnWriter.DATA_OFFSET + (long) sz * numRows) {
      throw new ISE("Column does not have the correct length");
    }
  }

  private static boolean getHasNulls(final Memory memoryRange)
  {
    return memoryRange.getByte(Byte.BYTES) != 0;
  }

  private static class LongFrameColumn implements NumericColumn
  {
    private final Frame frame;
    private final boolean hasNulls;
    private final int sz;
    private final Memory memory;
    private final long memoryPosition;

    private LongFrameColumn(
        final Frame frame,
        final boolean hasNulls,
        final Memory memory
    )
    {
      this.frame = frame;
      this.hasNulls = hasNulls;
      this.sz = LongFrameColumnWriter.valueSize(hasNulls);
      this.memory = memory;
      this.memoryPosition = LongFrameColumnWriter.DATA_OFFSET;
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(final ReadableOffset offset)
    {
      return new LongColumnSelector()
      {
        @Override
        public long getLong()
        {
          assert NullHandling.replaceWithDefault() || !isNull();
          return LongFrameColumn.this.getLong(frame.physicalRow(offset.getOffset()));
        }

        @Override
        public boolean isNull()
        {
          return LongFrameColumn.this.isNull(frame.physicalRow(offset.getOffset()));
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // Do nothing.
        }
      };
    }

    @Override
    public VectorValueSelector makeVectorValueSelector(final ReadableVectorOffset theOffset)
    {
      class LongFrameColumnVectorValueSelector extends BaseLongVectorValueSelector
      {
        private final long[] longVector;
        private final boolean[] nullVector;

        private int id = ReadableVectorInspector.NULL_ID;

        private LongFrameColumnVectorValueSelector()
        {
          super(theOffset);
          this.longVector = new long[offset.getMaxVectorSize()];
          this.nullVector = hasNulls ? new boolean[offset.getMaxVectorSize()] : null;
        }

        @Nullable
        @Override
        public boolean[] getNullVector()
        {
          computeVectorsIfNeeded();
          return nullVector;
        }

        @Override
        public long[] getLongVector()
        {
          computeVectorsIfNeeded();
          return longVector;
        }

        private void computeVectorsIfNeeded()
        {
          if (id == offset.getId()) {
            return;
          }

          if (offset.isContiguous()) {
            final int start = offset.getStartOffset();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(i + start);
              longVector[i] = getLong(physicalRow);

              if (hasNulls) {
                nullVector[i] = isNull(physicalRow);
              }
            }
          } else {
            final int[] offsets = offset.getOffsets();

            for (int i = 0; i < offset.getCurrentVectorSize(); i++) {
              final int physicalRow = frame.physicalRow(offsets[i]);
              longVector[i] = getLong(physicalRow);

              if (hasNulls) {
                nullVector[i] = isNull(physicalRow);
              }
            }
          }

          id = offset.getId();
        }
      }

      return new LongFrameColumnVectorValueSelector();
    }

    @Override
    public int length()
    {
      return frame.numRows();
    }

    @Override
    public long getLongSingleValueRow(final int rowNum)
    {
      // Need bounds checking, since getLong(physicalRow) doesn't do it.
      if (rowNum < 0 || rowNum >= frame.numRows()) {
        throw new ISE("Row [%d] out of bounds", rowNum);
      }

      return getLong(frame.physicalRow(rowNum));
    }

    @Override
    public void close()
    {
      // Do nothing.
    }

    @Override
    public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }

    private boolean isNull(final int physicalRow)
    {
      if (hasNulls) {
        final long rowPosition = memoryPosition + (long) sz * physicalRow;
        return memory.getByte(rowPosition) != 0;
      } else {
        return false;
      }
    }

    private long getLong(final int physicalRow)
    {
      final long rowPosition = memoryPosition + (long) sz * physicalRow;

      if (hasNulls) {
        return memory.getLong(rowPosition + 1);
      } else {
        return memory.getLong(rowPosition);
      }
    }
  }
}
