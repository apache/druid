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
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.write.columnar.ComplexFrameMaker;
import org.apache.druid.frame.write.columnar.FrameColumnWriters;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;
import org.apache.druid.query.rowsandcols.column.accessor.ObjectColumnAccessorBase;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;

public class ComplexFrameColumnReader implements FrameColumnReader
{
  private final int columnNumber;

  ComplexFrameColumnReader(final int columnNumber)
  {
    this.columnNumber = columnNumber;
  }

  @Override
  public Column readRACColumn(Frame frame)
  {
    return new ColumnAccessorBasedColumn(makeComplexFrameColumn(frame));
  }

  @Override
  public ColumnPlus readColumn(final Frame frame)
  {
    final ComplexFrameColumn frameCol = makeComplexFrameColumn(frame);

    return new ColumnPlus(
        frameCol,
        new ColumnCapabilitiesImpl()
            .setType(frameCol.getType())
            .setHasMultipleValues(false),
        frame.numRows()
    );
  }

  @Nonnull
  private ComplexFrameColumn makeComplexFrameColumn(Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    validate(memory, frame.numRows());

    final int typeNameLength = memory.getInt(ComplexFrameMaker.TYPE_NAME_LENGTH_POSITION);
    final byte[] typeNameBytes = new byte[typeNameLength];

    memory.getByteArray(ComplexFrameMaker.TYPE_NAME_POSITION, typeNameBytes, 0, typeNameLength);

    final String typeName = StringUtils.fromUtf8(typeNameBytes);
    final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);

    if (serde == null) {
      throw new ISE("Cannot read column with complexTypeName[%s]", typeName);
    }

    final long startOfOffsetSection = Byte.BYTES + Integer.BYTES + typeNameLength;
    final long startOfDataSection = startOfOffsetSection + (long) frame.numRows() * Integer.BYTES;

    return new ComplexFrameColumn(
        frame,
        serde,
        memory,
        startOfOffsetSection,
        startOfDataSection
    );
  }

  private void validate(final Memory region, final int numRows)
  {
    if (region.getCapacity() < ComplexFrameMaker.TYPE_NAME_POSITION) {
      throw new ISE("Column is not big enough for a header");
    }

    final byte typeCode = region.getByte(0);
    if (typeCode != FrameColumnWriters.TYPE_COMPLEX) {
      throw new ISE("Column does not have the correct type code");
    }

    final int typeNameLength = region.getInt(ComplexFrameMaker.TYPE_NAME_LENGTH_POSITION);
    if (region.getCapacity() <
        ComplexFrameMaker.TYPE_NAME_POSITION + typeNameLength + (long) numRows * Integer.BYTES) {
      throw new ISE("Column is missing offset section");
    }
  }

  private static class ComplexFrameColumn extends ObjectColumnAccessorBase implements ComplexColumn
  {
    private final Frame frame;
    private final ComplexMetricSerde serde;
    private final Class<?> clazz;
    private final Memory memory;
    private final long startOfOffsetSection;
    private final long startOfDataSection;

    private ComplexFrameColumn(
        final Frame frame,
        final ComplexMetricSerde serde,
        final Memory memory,
        final long startOfOffsetSection,
        final long startOfDataSection
    )
    {
      this.frame = frame;
      this.serde = serde;
      //noinspection deprecation
      this.clazz = serde.getObjectStrategy().getClazz();
      this.memory = memory;
      this.startOfOffsetSection = startOfOffsetSection;
      this.startOfDataSection = startOfDataSection;
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(final ReadableOffset offset)
    {
      return new ObjectColumnSelector<>()
      {
        @Nullable
        @Override
        public Object getObject()
        {
          return ComplexFrameColumn.this.getObjectForPhysicalRow(frame.physicalRow(offset.getOffset()));
        }

        @Override
        public Class<?> classOfObject()
        {
          return clazz;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // Do nothing.
        }
      };
    }

    @Override
    public Class<?> getClazz()
    {
      return serde.getClass();
    }

    @Override
    public String getTypeName()
    {
      return serde.getTypeName();
    }

    @Override
    @Nullable
    public Object getRowValue(int rowNum)
    {
      // Need bounds checking, since getObjectForPhysicalRow doesn't do it.
      if (rowNum < 0 || rowNum >= frame.numRows()) {
        throw new ISE("Row [%d] out of bounds", rowNum);
      }

      return getObjectForPhysicalRow(frame.physicalRow(rowNum));
    }

    @Override
    public int getLength()
    {
      return (int) frame.numBytes();
    }

    @Override
    public void close()
    {
      // Do nothing.
    }

    @Override
    public ColumnType getType()
    {
      return ColumnType.ofComplex(serde.getTypeName());
    }

    @Override
    public int numRows()
    {
      return getLength();
    }

    @Override
    protected Object getVal(int rowNum)
    {
      return getRowValue(rowNum);
    }

    @Override
    protected Comparator<Object> getComparator()
    {
      return serde.getTypeStrategy();
    }

    @Nullable
    private Object getObjectForPhysicalRow(final int physicalRow)
    {
      final long endOffset =
          startOfDataSection + memory.getInt(startOfOffsetSection + (long) Integer.BYTES * physicalRow);
      final long startOffset;

      if (physicalRow == 0) {
        startOffset = startOfDataSection;
      } else {
        startOffset =
            startOfDataSection + memory.getInt(startOfOffsetSection + (long) Integer.BYTES * (physicalRow - 1));
      }

      if (memory.getByte(startOffset) == ComplexFrameMaker.NULL_MARKER) {
        return null;
      } else {
        final int payloadLength = Ints.checkedCast(endOffset - startOffset - Byte.BYTES);
        final byte[] complexBytes = new byte[payloadLength];
        memory.getByteArray(startOffset + Byte.BYTES, complexBytes, 0, payloadLength);
        return serde.fromBytes(complexBytes, 0, complexBytes.length);
      }
    }
  }
}
