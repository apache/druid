package org.apache.druid.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.segment.BaseNullableColumnValueSelector;

public abstract class NumericFieldWriter implements FieldWriter
{
  public static final byte NULL_BYTE = 0x00;
  public static final byte NOT_NULL_BYTE = 0x01;

  private final BaseNullableColumnValueSelector selector;

  public NumericFieldWriter(final BaseNullableColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public long writeTo(WritableMemory memory, long position, long maxSize)
  {
    int size = getNumericSize() + Byte.BYTES;

    if (maxSize < size) {
      return -1;
    }

    if (selector.isNull()) {
      memory.putByte(position, NULL_BYTE);
      writeNullToMemory(memory, position + Byte.BYTES);
    } else {
      memory.putByte(position, NOT_NULL_BYTE);
      writeSelectorToMemory(memory, position + Byte.BYTES);
    }

    return size;
  }

  @Override
  public void close()
  {
    // Nothing to do
  }

  public abstract int getNumericSize();

  public abstract void writeSelectorToMemory(WritableMemory memory, long position);

  public abstract void writeNullToMemory(WritableMemory memory, long position);
}
