package io.druid.segment.data;

import com.metamx.common.IAE;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Currently this only support big endian
 *
 * Size 3 and 6 are not used due to performance reasons, consider enable them if compressed size becomes important
 *
 * Note 3 and 6 in the current state would cause IndexOutofBoundException at the end of file, which would enter the
 * catch block in safeGetInt. This will have a significant performance impact due to java try catch optimization.
 * Either change implementation or prevent the situation by writing some empty bytes when closing.
 */
public class VSizeLongSerde
{

  public static final int SUPPORTED_SIZE[] = {1, 2, /*3,*/ 4, /*6,*/ 8, 12, 16, 20, 24, 32, 40, 48, 56, 64};

  public static int getBitsForMax(long value)
  {
    if (value < 0) {
      throw new IAE("maxValue[%s] must be positive", value);
    }
    byte numBytes = 0;
    long maxValue = 1;
    for (int i = 0; i < SUPPORTED_SIZE.length; i++) {
      while (numBytes < SUPPORTED_SIZE[i] && maxValue < Long.MAX_VALUE / 2) {
        numBytes++;
        maxValue *= 2;
      }
      if (value <= maxValue || maxValue >= Long.MAX_VALUE / 2) {
        return SUPPORTED_SIZE[i];
      }
    }
    return 64;
  }

  public static LongSerializer getSerializer (int longSize, OutputStream output) {
    switch (longSize) {
      case 1: return new Size1Ser(output);
      case 2: return new Size2Ser(output);
//      case 3: return new Size3Ser(output);
      case 4: return new Mult4Ser(output, 0);
//      case 6: return new Size6Ser(output);
      case 8: return new Mult8Ser(output, 1);
      case 12: return new Mult4Ser(output, 1);
      case 16: return new Mult8Ser(output, 2);
      case 20: return new Mult4Ser(output, 2);
      case 24: return new Mult8Ser(output, 3);
      case 32: return new Mult8Ser(output, 4);
      case 40: return new Mult8Ser(output, 5);
      case 48: return new Mult8Ser(output, 6);
      case 56: return new Mult8Ser(output, 7);
      case 64: return new Mult8Ser(output, 8);
      default:
        throw new IAE("Unsupported size %s", longSize);
    }
  }

  public static LongDeserializer getDeserializer (int longSize, ByteBuffer buffer, int bufferOffset) {
    buffer.order(ByteOrder.BIG_ENDIAN);
    switch (longSize) {
      case 1: return new Size1Des(buffer, bufferOffset);
      case 2: return new Size2Des(buffer, bufferOffset);
//      case 3: return new Size3Des(buffer, bufferOffset);
      case 4: return new Size4Des(buffer, bufferOffset);
//      case 6: return new Size6Des(buffer, bufferOffset);
      case 8: return new Size8Des(buffer, bufferOffset);
      case 12: return new Size12Des(buffer, bufferOffset);
      case 16: return new Size16Des(buffer, bufferOffset);
      case 20: return new Size20Des(buffer, bufferOffset);
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

  public interface LongSerializer extends Closeable {
    void write (long value) throws IOException;
  }

  private static final class Size1Ser implements LongSerializer {
    OutputStream output;
    byte buffer = 0;
    int count = 0;
    public Size1Ser(OutputStream output) {
     this.output = output;
    }
    @Override
    public void write(long value) throws IOException
    {
      if (count == 8) {
        output.write(buffer);
        count = 0;
      }
      buffer = (byte)((buffer << 1) | (value & 1));
      count++;
    }

    @Override
    public void close() throws IOException
    {
      output.write(buffer << (8 - count));
      output.flush();
    }
  }

  private static final class Size2Ser implements LongSerializer {
    OutputStream output;
    byte buffer = 0;
    int count = 0;
    public Size2Ser(OutputStream output) {
      this.output = output;
    }
    @Override
    public void write(long value) throws IOException
    {
      if (count == 8) {
        output.write(buffer);
        count = 0;
      }
      buffer = (byte)((buffer << 2) | (value & 3));
      count += 2;
    }

    @Override
    public void close() throws IOException
    {
      output.write(buffer << (8 - count));
      output.flush();
    }
  }

  private static final class Size3Ser implements LongSerializer {
    OutputStream output;
    int buffer = 0;
    int count = 0;
    public Size3Ser(OutputStream output) {
      this.output = output;
    }
    @Override
    public void write(long value) throws IOException
    {
      if (count == 8) {
        output.write(buffer >> 16);
        output.write(buffer >> 8);
        output.write(buffer);
        count = 0;
      }
      buffer = (int)((buffer << 3) | (value & 7));
      count++;
    }

    @Override
    public void close() throws IOException
    {
      int unwrittenBytes = (count * 3 - 1) / 8;
      int shift = 7 - ((count * 3 - 1) % 8);
      for (int i = unwrittenBytes; i >= 0; i--) {
        output.write(buffer << shift >> (i * 8));
      }
      output.flush();
    }
  }

  private static final class Size6Ser implements LongSerializer {
    OutputStream output;
    int buffer = 0;
    int count = 0;
    public Size6Ser(OutputStream output) {
      this.output = output;
    }
    @Override
    public void write(long value) throws IOException
    {
      if (count == 4) {
        output.write(buffer >> 16);
        output.write(buffer >> 8);
        output.write(buffer);
        count = 0;
      }
      buffer = (int)((buffer << 6) | (value & 0x7F));
      count++;
    }

    @Override
    public void close() throws IOException
    {
      int unwrittenBytes = (count * 6 - 1) / 8;
      int shift = 7 - ((count * 6 - 1) % 8);
      for (int i = unwrittenBytes; i >= 0; i--) {
        output.write(buffer << shift >> (i * 8));
      }
      output.flush();
    }
  }

  private static final class Mult4Ser implements LongSerializer {
    OutputStream output;
    int numBytes;
    byte buffer = 0;
    boolean first = true;
    public Mult4Ser(OutputStream output, int numBytes) {
      this.output = output;
      this.numBytes = numBytes;
    }
    @Override
    public void write(long value) throws IOException
    {
      int shift = 0;
      if (first) {
        shift = 4;
        buffer = (byte)value;
        first = false;
      } else {
        buffer = (byte)((buffer << 4) | ((value >> (numBytes << 3)) & 0xF));
        output.write(buffer);
        first = true;
      }
      for (int i = numBytes - 1; i >= 0; i--) {
        output.write((byte)(value >>> (i * 8 + shift)));
      }
    }
    @Override
    public void close() throws IOException
    {
      if (!first) {
        output.write(buffer << 4);
      }
      output.flush();
    }
  }

  private static final class Mult8Ser implements LongSerializer {
    OutputStream output;
    int numBytes;
    public Mult8Ser(OutputStream output, int numBytes) {
      this.output = output;
      this.numBytes = numBytes;
    }
    @Override
    public void write(long value) throws IOException
    {
      for (int i = numBytes - 1; i >= 0; i--) {
        output.write((byte)(value >>> (i * 8)));
      }
    }
    @Override
    public void close() throws IOException
    {
      output.flush();
    }
  }

  public interface LongDeserializer {
    long get (int index);
  }

  private static final class Size1Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size1Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      int shift = 7 - (index & 7);
      return (buffer.get(offset + (index >> 3)) >> shift) & 1;
    }
  }

  private static final class Size2Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size2Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      int shift = 6 - ((index & 3) << 1);
      return (buffer.get(offset + (index >> 2)) >> shift) & 3;
    }
  }

  private static final class Size3Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size3Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      int shift = 29 - (index & 7) * 3;
      return (safeGetInt(buffer, offset + (index >> 3) * 3) >> shift) & 7;
    }
  }

  private static final class Size4Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size4Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      int shift = ((index + 1) & 1) << 2;
      return (buffer.get(offset + (index >> 1)) >> shift) & 0xF;
    }
  }

  private static final class Size6Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size6Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      int shift = 26 - (index & 3) * 6;
      return (safeGetInt(buffer, offset + (index >> 2) * 3) >> shift) & 0x3F;
    }
  }

  private static final class Size8Des implements LongDeserializer {
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

  private static final class Size12Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size12Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      int shift = ((index + 1) & 1) << 2;
      int offset = (index * 3) >> 1;
      return (safeGetShort(buffer, this.offset + offset) >> shift) & 0xFFF;
    }
  }

  private static final class Size16Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size16Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    public long get(int index)
    {
      return safeGetShort(buffer, offset + (index << 1)) & 0xFFFF;
    }
  }

  private static final class Size20Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size20Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      int shift = ((index + 1) & 1) << 2;
      int offset = (index * 5) >> 1;
      return (safeGetInt(buffer, this.offset + offset - 1) >> shift) & 0xFFFFF;
    }
  }

  private static final class Size24Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size24Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return safeGetInt(buffer, offset + index * 3 - 1) & 0xFFFFFF;
    }
  }

  private static final class Size32Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size32Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return safeGetInt(buffer, offset + (index << 2)) & 0xFFFFFFFFL;
    }
  }

  private static final class Size40Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size40Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return safeGetLong(buffer, offset + index * 5 - 3) & 0xFFFFFFFFFFL;
    }
  }

  private static final class Size48Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size48Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return safeGetLong(buffer, offset + index * 6 - 2) & 0xFFFFFFFFFFFFL;
    }
  }

  private static final class Size56Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size56Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return safeGetLong(buffer, offset + index * 7 - 1) & 0xFFFFFFFFFFFFFFL;
    }
  }

  private static final class Size64Des implements LongDeserializer {
    final ByteBuffer buffer;
    final int offset;
    public Size64Des(ByteBuffer buffer, int bufferOffset) {
      this.buffer = buffer;
      this.offset = bufferOffset;
    }
    @Override
    public long get(int index)
    {
      return safeGetLong(buffer, offset + (index << 3));
    }
  }

  private static byte safeGet(ByteBuffer buffer, int index) {
    try {
      return buffer.get(index);
    } catch (IndexOutOfBoundsException ex) {
      return 0;
    }
  }

  private static short safeGetShort(ByteBuffer buffer, int index) {
    try {
      return buffer.getShort(index);
    } catch (IndexOutOfBoundsException ex) {
      if (index >= buffer.limit()) {
        throw ex;
      }
      return (short)((safeGet(buffer, index) << 8) | (safeGet(buffer, index + 1) & 0xFF));
    }
  }

  private static int safeGetInt(ByteBuffer buffer, int index) {
    try {
      return buffer.getInt(index);
    } catch (IndexOutOfBoundsException ex) {
      if (index >= buffer.limit()) {
        throw ex;
      }
      return ((safeGet(buffer, index) << 24) |
              ((safeGet(buffer, index + 1) & 0xFF) << 16) |
              ((safeGet(buffer, index + 2) & 0xFF) << 8) |
              ((safeGet(buffer, index + 3) & 0xFF)));
    }
  }

  private static long safeGetLong(ByteBuffer buffer, int index) {
    try {
      return buffer.getLong(index);
    } catch (IndexOutOfBoundsException ex) {
      if (index >= buffer.limit()) {
        throw ex;
      }
      return (((long)safeGet(buffer, index) << 56) |
              (((long)safeGet(buffer, index + 1) & 0xFF) << 48) |
              (((long)safeGet(buffer, index + 2) & 0xFF) << 40) |
              (((long)safeGet(buffer, index + 3) & 0xFF) << 32) |
              (((long)safeGet(buffer, index + 4) & 0xFF) << 24) |
              (((long)safeGet(buffer, index + 5) & 0xFF) << 16) |
              (((long)safeGet(buffer, index + 6) & 0xFF) << 8) |
              (((long)safeGet(buffer, index + 7) & 0xFF)));
    }
  }

}
