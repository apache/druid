package org.apache.druid.frame.field;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

public class NumericArrayFieldReader implements FieldReader
{
  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer
  )
  {
    return null;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      @Nullable ExtractionFn extractionFn
  )
  {
    throw DruidException.defensive("Cannot call makeDimensionSelector on field of type [%s]", array);
  }

  @Override
  public boolean isNull(Memory memory, long position)
  {
    final byte firstByte = memory.getByte(position);
    return firstByte == NumericArrayFieldWriter.NULL_ROW;
  }

  @Override
  public boolean isComparable()
  {
    return true;
  }

  private static class Selector implements BaseObjectColumnValueSelector
  {

  }
}
