package io.druid.segment.data;

import com.metamx.common.IAE;

import java.nio.ByteBuffer;

public class VSizeLongSerde
{
  //TODO
  public static byte getBitsForMax(long maxValue)
  {
    if (maxValue < 0) {
      throw new IAE("maxValue[%s] must be positive", maxValue);
    }
    byte numBytes = 1;
    while (maxValue > 0) {

    }
    if (maxValue <= 0xFF) {
      numBytes = 1;
    }
    else if (maxValue <= 0xFFFF) {
      numBytes = 2;
    }
    else if (maxValue <= 0xFFFFFF) {
      numBytes = 3;
    }
    return numBytes;
  }

  public static LongDeserializer getDeserializer (int longSize, ByteBuffer buffer, int bufferOffset) {
    switch (longSize) {
      case 8: return new Size8Des(buffer, bufferOffset);
      case 16: return new Size16Des(buffer, bufferOffset);
      case 24: return new Size24Des(buffer, bufferOffset);
      case 32: return new Size32Des(buffer, bufferOffset);
      case 40: return new Size40Des(buffer, bufferOffset);
      case 48: return new Size48Des(buffer, bufferOffset);
      case 56: return new Size56Des(buffer, bufferOffset);
      case 64: return new Size64Des(buffer, bufferOffset);
      default:
        throw new IAE("Unsupported size %s", longSize);
    }
  }

  interface LongDeserializer {
    long get (int index);
  }

  private static class Size8Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size8Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return buffer.get(offset + index) & 0xFF;
    }
  }

  private static class Size16Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size16Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    public long get(int index)
    {
      return buffer.getShort(offset + index << 1) & 0xFFFF;
    }
  }

  private static class Size24Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size24Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return buffer.getInt(offset + index * 3)  >>> 8;
    }
  }

  private static class Size32Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size32Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return buffer.getInt(offset + index << 2) & 0xFFFFFFFFL;
    }
  }

  private static class Size40Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size40Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return buffer.getLong(offset + index * 5) >>> 24;
    }
  }

  private static class Size48Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size48Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return buffer.getLong(offset + index * 6) >>> 16;
    }
  }

  private static class Size56Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size56Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return buffer.getLong(offset + index * 7) >>> 8;
    }
  }

  private static class Size64Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size64Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return buffer.getLong(offset + index << 3);
    }
  }

}
