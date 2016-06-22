package io.druid.segment.data;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Storage Format v1:
 * Byte 1 : version
 * Byte 2 - 9 : base value
 * Byte 10 - 13 : number of bits per value
 */
public class DeltaEncodingFormatSerde
{
  private static final byte V1 = 0x1;

  public static class DeltaEncodingReader implements CompressionFactory.LongEncodingFormatReader
  {

    private ByteBuffer buffer;
    private final long base;
    private final int bitsPerValue;
    private VSizeLongSerde.LongDeserializer deserializer;

    public DeltaEncodingReader(ByteBuffer fromBuffer) {
      this.buffer = fromBuffer.asReadOnlyBuffer();
      byte version = buffer.get();
      if (version == V1) {
        base = buffer.getLong();
        bitsPerValue = buffer.getInt();
        fromBuffer.position(buffer.position());
        deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
      } else {
        throw new IAE("Unknown version[%s]", version);
      }
    }

    public DeltaEncodingReader(ByteBuffer buffer, long base, int bitsPerValue) {
      this.buffer = buffer.asReadOnlyBuffer();
      this.base = base;
      this.bitsPerValue = bitsPerValue;
      deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
    }

    @Override
    public void setBuffer(ByteBuffer buffer)
    {
      deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
    }

    @Override
    public long read(int index)
    {
      return base + deserializer.get(index);
    }

    @Override
    public int numBytes(int values)
    {
      return VSizeLongSerde.getSerializedSize(bitsPerValue, values);
    }

    @Override
    public CompressionFactory.LongEncodingFormatReader duplicate()
    {
      return new DeltaEncodingReader(buffer, base, bitsPerValue);
    }
  }

  public static class DeltaEncodingWriter implements CompressionFactory.LongEncodingFormatWriter
  {

    private final long base;
    private VSizeLongSerde.LongSerializer serializer;
    private int bitsPerValue;

    public DeltaEncodingWriter(long base, long delta) {
      this.base = base;
      this.bitsPerValue = VSizeLongSerde.getBitsForMax(delta + 1);
    }

    @Override
    public void setBuffer(ByteBuffer buffer)
    {
      serializer = VSizeLongSerde.getSerializer(bitsPerValue, buffer, buffer.position());
    }

    @Override
    public void setOutputStream(OutputStream output)
    {
      serializer = VSizeLongSerde.getSerializer(bitsPerValue, output);
    }

    @Override
    public void write(long value) throws IOException
    {
      serializer.write(value - base);
    }

    public void putMeta(OutputStream metaOut, CompressedObjectStrategy.CompressionStrategy strategy) throws IOException
    {
      metaOut.write(strategy.getId() - 126);
      metaOut.write(CompressionFactory.LongEncodingFormat.DELTA.getId());
      metaOut.write(V1);
      metaOut.write(Longs.toByteArray(base));
      metaOut.write(Ints.toByteArray(bitsPerValue));
    }

    @Override
    public int getBlockSize(int bytesPerBlock)
    {
      return VSizeLongSerde.getBlockSize(bitsPerValue, bytesPerBlock);
    }

    @Override
    public int getNumBytes(int values)
    {
      return VSizeLongSerde.getSerializedSize(bitsPerValue, values);
    }

    @Override
    public void close() throws IOException
    {
      if (serializer != null) {
        serializer.close();
      }
    }
  }
}
