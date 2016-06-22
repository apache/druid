package io.druid.segment.data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class VSizeCompressedObjectStrategy extends CompressedObjectStrategy<ByteBuffer>
{

  private final int expectedBytes;

  public static VSizeCompressedObjectStrategy getBufferForOrder(final ByteOrder order, final CompressionStrategy compression, final int expectedBytes)
  {
    return new VSizeCompressedObjectStrategy(order, compression, expectedBytes);
  }

  protected VSizeCompressedObjectStrategy(
      ByteOrder order,
      CompressionStrategy compression,
      int expectedBytes
  )
  {
    super(order, new BufferConverter<ByteBuffer>()
    {
      @Override
      public ByteBuffer convert(ByteBuffer buf)
      {
        return buf;
      }

      @Override
      public int compare(ByteBuffer lhs, ByteBuffer rhs)
      {
        return CompressedByteBufferObjectStrategy.ORDERING.compare(lhs, rhs);
      }

      @Override
      public int sizeOf(int count)
      {
        return count; // 1 byte per element
      }

      @Override
      public ByteBuffer combine(ByteBuffer into, ByteBuffer from)
      {
        return into.put(from);
      }
    }, compression);

    this.expectedBytes = expectedBytes;
  }

  @Override
  protected ByteBuffer bufferFor(ByteBuffer val)
  {
    return ByteBuffer.allocate(expectedBytes).order(order);
  }

  @Override
  protected void decompress(ByteBuffer buffer, int numBytes, ByteBuffer buf)
  {
    decompressor.decompress(buffer, numBytes, buf, expectedBytes);
  }
}
