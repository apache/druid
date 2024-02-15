package org.apache.druid.frame.read.columnar;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.write.columnar.LongArrayFrameColumnWriter;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.accessor.ObjectColumnAccessorBase;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ReadableOffset;

import java.io.IOException;
import java.util.Comparator;

public class LongArrayFrameColumnReader implements FrameColumnReader
{
  private final byte typeCode;
  private final int columnNumber;

  public LongArrayFrameColumnReader(byte typeCode, int columnNumber)
  {
    this.columnNumber = columnNumber;
    this.typeCode = typeCode;
  }

  @Override
  public Column readRACColumn(Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    validate(memory);
    throw DruidException.defensive("Multivalue not yet handled by RAC");
  }

  @Override
  public ColumnPlus readColumn(Frame frame)
  {
    final Memory memory = frame.region(columnNumber);
    validate(memory);
  }

  private void validate(final Memory region) {
    if (region.getCapacity() < LongArrayFrameColumnWriter.DATA_OFFSET) {
      throw DruidException.defensive("Column[%s] is not big enough for a header", columnNumber);
    }
    final byte typeCode = region.getByte(0);
    if (typeCode != this.typeCode) {
      throw DruidException.defensive(
          "Column[%s] does not have the correct type code; expected[%s], got[%s]",
          columnNumber,
          this.typeCode,
          typeCode
      );
    }
  }

  private static class LongArrayFrameColumn extends ObjectColumnAccessorBase implements BaseColumn
  {
    @Override
    public void close() throws IOException
    {

    }

    @Override
    public ColumnType getType()
    {
      return null;
    }

    @Override
    public int numRows()
    {
      return 0;
    }

    @Override
    protected Object getVal(int rowNum)
    {
      return null;
    }

    @Override
    protected Comparator<Object> getComparator()
    {
      return null;
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
    {
      return null;
    }
  }
}
