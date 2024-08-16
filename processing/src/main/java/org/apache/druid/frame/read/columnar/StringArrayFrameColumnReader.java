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
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.write.columnar.FrameColumnWriters;
import org.apache.druid.frame.write.columnar.StringFrameColumnWriter;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessorBasedColumn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ReadableOffset;

/**
 * Reader for {@link ColumnType#STRING_ARRAY}.
 */
public class StringArrayFrameColumnReader implements FrameColumnReader
{
  final int columnNumber;

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

    // String arrays always store multiple values per row.
    assert isMultiValue(memory);
    final long positionOfLengths = getStartOfStringLengthSection(frame.numRows());
    final long positionOfPayloads = getStartOfStringDataSection(memory, frame.numRows());

    StringFrameColumnReader.StringFrameColumn frameCol = new StringFrameColumnReader.StringFrameColumn(
        frame,
        true,
        memory,
        positionOfLengths,
        positionOfPayloads,
        true
    );

    return new ColumnAccessorBasedColumn(frameCol);
  }

  @Override
  public ColumnPlus readColumn(final Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    validate(memory);

    // String arrays always store multiple values per row.
    assert isMultiValue(memory);
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

  static boolean isMultiValue(final Memory memory)
  {
    return memory.getByte(1) == 1;
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
    final int totalNumValues = FrameColumnReaderUtils.getAdjustedCumulativeRowLength(
        memory,
        getStartOfCumulativeLengthSection(),
        numRows - 1
    );

    return getStartOfStringLengthSection(numRows) + (long) Integer.BYTES * totalNumValues;
  }

  private static class StringArrayFrameColumn implements BaseColumn
  {
    private final StringFrameColumnReader.StringFrameColumn delegate;

    StringArrayFrameColumn(
        Frame frame,
        Memory memory,
        long startOfStringLengthSection,
        long startOfStringDataSection
    )
    {
      this.delegate = new StringFrameColumnReader.StringFrameColumn(
          frame,
          true,
          memory,
          startOfStringLengthSection,
          startOfStringDataSection,
          true
      );
    }

    @Override
    @SuppressWarnings("rawtypes")
    public ColumnValueSelector makeColumnValueSelector(ReadableOffset offset)
    {
      return delegate.makeDimensionSelectorInternal(offset, null);
    }

    @Override
    public void close()
    {
      delegate.close();
    }
  }
}
