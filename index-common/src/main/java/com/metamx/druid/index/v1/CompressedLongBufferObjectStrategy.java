package com.metamx.druid.index.v1;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
*/
public class CompressedLongBufferObjectStrategy extends CompressedObjectStrategy<LongBuffer>
{
  public static CompressedLongBufferObjectStrategy getBufferForOrder(ByteOrder order)
  {
    return new CompressedLongBufferObjectStrategy(order);
  }

  private CompressedLongBufferObjectStrategy(final ByteOrder order)
  {
    super(
        order,
        new BufferConverter<LongBuffer>()
        {
          @Override
          public LongBuffer convert(ByteBuffer buf)
          {
            return buf.asLongBuffer();
          }

          @Override
          public int compare(LongBuffer lhs, LongBuffer rhs)
          {
            return Ordering.natural().nullsFirst().compare(lhs, rhs);
          }

          @Override
          public int sizeOf(int count)
          {
            return count * Longs.BYTES;
          }

          @Override
          public LongBuffer combine(ByteBuffer into, LongBuffer from)
          {
            return into.asLongBuffer().put(from);
          }
        }
    );
  }


}
