package io.druid.segment.data;

import com.google.common.primitives.Longs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
 * There is no header for this format for backward compatibility
 */
public class LongsEncodingFormatSerde
{

  public static class LongsEncodingReader implements CompressionFactory.LongEncodingFormatReader
  {
    private LongBuffer buffer;
    private ByteOrder order;

    public LongsEncodingReader(ByteBuffer fromBuffer, ByteOrder order) {
      this.buffer = fromBuffer.asReadOnlyBuffer().order(order).asLongBuffer();
      this.order = order;
    }

    public LongsEncodingReader(LongBuffer buffer, ByteOrder order) {
      this.buffer = buffer;
      this.order = order;
    }

    @Override
    public void setBuffer(ByteBuffer buffer)
    {
      this.buffer = buffer.order(order).asLongBuffer();
    }

    @Override
    public long read(int index)
    {
      return buffer.get(buffer.position() + index);
    }

    @Override
    public int numBytes(int values)
    {
      return values * Longs.BYTES;
    }

    @Override
    public CompressionFactory.LongEncodingFormatReader duplicate()
    {
      return new LongsEncodingReader(buffer, order);
    }
  }

  public static class LongsEncodingWriter implements CompressionFactory.LongEncodingFormatWriter
  {

    private final ByteBuffer orderBuffer;
    private final ByteOrder order;
    private ByteBuffer outBuffer = null;
    private OutputStream outStream = null;

    public LongsEncodingWriter (ByteOrder order) {
      this.order = order;
      orderBuffer = ByteBuffer.allocate(Longs.BYTES);
      orderBuffer.order(order);
    }

    @Override
    public void setBuffer(ByteBuffer buffer)
    {
      outStream = null;
      outBuffer = buffer;
      outBuffer.order(order);
    }

    @Override
    public void setOutputStream(OutputStream output)
    {
      outBuffer = null;
      outStream = output;
    }

    @Override
    public void write(long value) throws IOException
    {
      if (outBuffer != null) {
        outBuffer.putLong(value);
      }
      if (outStream != null) {
        orderBuffer.rewind();
        orderBuffer.putLong(value);
        outStream.write(orderBuffer.array());
      }
    }

    @Override
    public void close() throws IOException
    {
      if (outStream != null) {
        outStream.close();
      }
    }

    @Override
    public void putMeta(OutputStream metaOut, CompressedObjectStrategy.CompressionStrategy strategy) throws IOException
    {
      metaOut.write(strategy.getId());
    }

    @Override
    public int getBlockSize(int bytesPerBlock)
    {
      return bytesPerBlock / Longs.BYTES;
    }

    @Override
    public int getNumBytes(int values)
    {
      return values * Longs.BYTES;
    }
  }
}
