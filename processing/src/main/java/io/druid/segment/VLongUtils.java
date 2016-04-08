package io.druid.segment;

import java.nio.ByteBuffer;

/**
 * from org.apache.hadoop.io.WritableUtils. modified for using ByteBuffer
 */
public class VLongUtils
{
  /**
   * Serializes an integer to a binary stream with zero-compressed encoding.
   * For -120 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * integer is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -121 and -124, the following integer
   * is positive, with number of bytes that follow are -(v+120).
   * If the first byte value v is between -125 and -128, the following integer
   * is negative, with number of bytes that follow are -(v+124). Bytes are
   * stored in the high-non-zero-byte-first order.
   *
   * @param buf ByteBuffer output buffer
   * @param i Integer to be serialized
   * @throws java.io.IOException
   */
  public static void writeVInt(ByteBuffer buf, int i)
  {
    writeVLong(buf, i);
  }

  /**
   * Serializes a long to a binary stream with zero-compressed encoding.
   * For -112 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * long is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -113 and -120, the following long
   * is positive, with number of bytes that follow are -(v+112).
   * If the first byte value v is between -121 and -128, the following long
   * is negative, with number of bytes that follow are -(v+120). Bytes are
   * stored in the high-non-zero-byte-first order.
   *
   * @param buf ByteBuffer output buffer
   * @param i Long to be serialized
   */
  public static void writeVLong(ByteBuffer buf, long i)
  {
    if (i >= -112 && i <= 127) {
      buf.put((byte) i);
      return;
    }

    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    buf.put((byte) len);

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      buf.put((byte) ((i & mask) >> shiftbits));
    }
  }

  /**
   * Reads a zero-compressed encoded long from input buffer and returns it.
   *
   * @param buf ByteBuffer input buffer
   * @return deserialized long from buffer.
   */
  public static long readVLong(ByteBuffer buf) {
    byte firstByte = buf.get();
    int len = decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = buf.get();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return isNegativeVInt(firstByte) ? ~i : i;
  }

  /**
   * Reads a zero-compressed encoded integer from input buffer and returns it.
   *
   * @param buf ByteBuffer input buffer
   * @throws IllegalStateException the value is not fit for int
   * @return deserialized integer from buffer.
   */
  public static int readVInt(ByteBuffer buf)
  {
    long n = readVLong(buf);
    if (n > Integer.MAX_VALUE || n < Integer.MIN_VALUE) {
      throw new IllegalStateException("value too long to fit in integer");
    }
    return (int)n;
  }

  /**
   * Given the first byte of a vint/vlong, determine the sign
   * @param value the first byte
   * @return is the value negative
   */
  public static boolean isNegativeVInt(byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }

  /**
   * Parse the first byte of a vint/vlong to determine the number of bytes
   * @param value the first byte of the vint/vlong
   * @return the total number of bytes (1 to 9)
   */
  public static int decodeVIntSize(byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }
}
