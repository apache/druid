package org.apache.druid.frame.field;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.segment.ColumnValueSelector;

public class LongArrayFieldReader extends NumericArrayFieldReader
{
  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer
  )
  {
    return new NumericArrayFieldReader.Selector<Long>(memory, fieldPointer)
    {

      @Override
      public Long getIndividualValueAtMemory(Memory memory, long position)
      {
        return new LongFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(position)).getLong();
      }

      @Override
      public int getIndividualFieldSize()
      {
        return Byte.BYTES + Long.BYTES;
      }
    };
  }
}
