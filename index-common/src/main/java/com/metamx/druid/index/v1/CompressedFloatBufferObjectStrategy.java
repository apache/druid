package com.metamx.druid.index.v1;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Floats;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

/**
*/
public class CompressedFloatBufferObjectStrategy extends CompressedObjectStrategy<FloatBuffer>
{
  public static CompressedFloatBufferObjectStrategy getBufferForOrder(ByteOrder order)
  {
    return new CompressedFloatBufferObjectStrategy(order);
  }

  private CompressedFloatBufferObjectStrategy(final ByteOrder order)
  {
    super(
        order,
        new BufferConverter<FloatBuffer>()
        {
          @Override
          public FloatBuffer convert(ByteBuffer buf)
          {
            return buf.asFloatBuffer();
          }

          @Override
          public int compare(FloatBuffer lhs, FloatBuffer rhs)
          {
            return Ordering.natural().nullsFirst().compare(lhs, rhs);
          }

          @Override
          public int sizeOf(int count)
          {
            return count * Floats.BYTES;
          }

          @Override
          public FloatBuffer combine(ByteBuffer into, FloatBuffer from)
          {
            return into.asFloatBuffer().put(from);
          }
        }
    );
  }
}
