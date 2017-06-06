/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.common.utils;

import com.google.common.io.ByteStreams;
import com.google.common.io.OutputSupplier;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.collections.IntList;
import io.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;

public class SerializerUtils
{

  /**
   * Writes the given long value into the given OutputStream in big-endian byte order, using the helperBuffer. Faster
   * alternative to out.write(Longs.toByteArray(value)), more convenient (sometimes) than wrapping the OutputStream into
   * {@link java.io.DataOutputStream}.
   *
   * @param helperBuffer a big-endian heap ByteBuffer with capacity of at least 8
   */
  public static void writeBigEndianLongToOutputStream(OutputStream out, long value, ByteBuffer helperBuffer)
      throws IOException
  {
    if (helperBuffer.order() != ByteOrder.BIG_ENDIAN || !helperBuffer.hasArray()) {
      throw new IllegalArgumentException("Expected writable, big-endian, heap byteBuffer");
    }
    helperBuffer.putLong(0, value);
    out.write(helperBuffer.array(), helperBuffer.arrayOffset(), Longs.BYTES);
  }

  /**
   * Writes the given long value into the given OutputStream in the native byte order, using the helperBuffer.
   *
   * @param helperBuffer a heap ByteBuffer with capacity of at least 8, with the native byte order
   */
  public static void writeNativeOrderedLongToOutputStream(OutputStream out, long value, ByteBuffer helperBuffer)
      throws IOException
  {
    if (helperBuffer.order() != ByteOrder.nativeOrder() || !helperBuffer.hasArray()) {
      throw new IllegalArgumentException("Expected writable heap byteBuffer with the native byte order");
    }
    helperBuffer.putLong(0, value);
    out.write(helperBuffer.array(), helperBuffer.arrayOffset(), Longs.BYTES);
  }

  /**
   * Writes the given int value into the given OutputStream in big-endian byte order, using the helperBuffer. Faster
   * alternative to out.write(Ints.toByteArray(value)), more convenient (sometimes) than wrapping the OutputStream into
   * {@link java.io.DataOutputStream}.
   *
   * @param helperBuffer a big-endian heap ByteBuffer with capacity of at least 4
   */
  public static void writeBigEndianIntToOutputStream(OutputStream out, int value, ByteBuffer helperBuffer)
      throws IOException
  {
    if (helperBuffer.order() != ByteOrder.BIG_ENDIAN || !helperBuffer.hasArray()) {
      throw new IllegalArgumentException("Expected writable, big-endian, heap byteBuffer");
    }
    helperBuffer.putInt(0, value);
    out.write(helperBuffer.array(), helperBuffer.arrayOffset(), Ints.BYTES);
  }

  /**
   * Writes the given int value into the given OutputStream in the native byte order, using the given helperBuffer.
   *
   * @param helperBuffer a heap ByteBuffer with capacity of at least 4, with the native byte order
   */
  public static void writeNativeOrderedIntToOutputStream(OutputStream out, int value, ByteBuffer helperBuffer)
      throws IOException
  {
    if (helperBuffer.order() != ByteOrder.nativeOrder() || !helperBuffer.hasArray()) {
      throw new IllegalArgumentException("Expected writable heap byteBuffer with the native byte order");
    }
    helperBuffer.putInt(0, value);
    out.write(helperBuffer.array(), helperBuffer.arrayOffset(), Ints.BYTES);
  }

  public <T extends OutputStream> void writeString(T out, String name) throws IOException
  {
    byte[] nameBytes = StringUtils.toUtf8(name);
    writeInt(out, nameBytes.length);
    out.write(nameBytes);
  }

  public void writeString(OutputSupplier<? extends OutputStream> supplier, String name) throws IOException
  {
    try (OutputStream out = supplier.getOutput()) {
      writeString(out, name);
    }
  }

  public void writeString(WritableByteChannel out, String name) throws IOException
  {
    byte[] nameBytes = StringUtils.toUtf8(name);
    writeInt(out, nameBytes.length);
    out.write(ByteBuffer.wrap(nameBytes));
  }

  public String readString(InputStream in) throws IOException
  {
    final int length = readInt(in);
    byte[] stringBytes = new byte[length];
    ByteStreams.readFully(in, stringBytes);
    return StringUtils.fromUtf8(stringBytes);
  }

  public String readString(ByteBuffer in) throws IOException
  {
    final int length = in.getInt();
    return StringUtils.fromUtf8(readBytes(in, length));
  }
  
  public byte[] readBytes(ByteBuffer in, int length) throws IOException
  {
    byte[] bytes = new byte[length];
    in.get(bytes);
    return bytes;
  }

  public void writeStrings(OutputStream out, String[] names) throws IOException
  {
    writeStrings(out, Arrays.asList(names));
  }

  public void writeStrings(OutputStream out, List<String> names) throws IOException
  {
    writeInt(out, names.size());

    for (String name : names) {
      writeString(out, name);
    }
  }

  public String[] readStrings(InputStream in) throws IOException
  {
    int length = readInt(in);

    String[] retVal = new String[length];

    for (int i = 0; i < length; ++i) {
      retVal[i] = readString(in);
    }

    return retVal;
  }

  public String[] readStrings(ByteBuffer in) throws IOException
  {
    int length = in.getInt();

    String[] retVal = new String[length];

    for (int i = 0; i < length; ++i) {
      retVal[i] = readString(in);
    }

    return retVal;
  }

  public void writeInt(OutputStream out, int intValue) throws IOException
  {
    out.write(Ints.toByteArray(intValue));
  }

  public void writeInt(WritableByteChannel out, int intValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(Ints.BYTES);
    buffer.putInt(intValue);
    buffer.flip();
    out.write(buffer);
  }

  public int readInt(InputStream in) throws IOException
  {
    byte[] intBytes = new byte[Ints.BYTES];

    ByteStreams.readFully(in, intBytes);

    return Ints.fromByteArray(intBytes);
  }

  public void writeInts(OutputStream out, int[] ints) throws IOException
  {
    writeInt(out, ints.length);

    for (int value : ints) {
      writeInt(out, value);
    }
  }

  public void writeInts(OutputStream out, IntList ints) throws IOException
  {
    writeInt(out, ints.length());

    for (int i = 0; i < ints.length(); i++) {
      writeInt(out, ints.get(i));
    }
  }

  public int[] readInts(InputStream in) throws IOException
  {
    int size = readInt(in);

    int[] retVal = new int[size];
    for (int i = 0; i < size; ++i) {
      retVal[i] = readInt(in);
    }

    return retVal;
  }

  public void writeLong(OutputStream out, long longValue) throws IOException
  {
    out.write(Longs.toByteArray(longValue));
  }

  public void writeLong(WritableByteChannel out, long longValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(Longs.BYTES);
    buffer.putLong(longValue);
    buffer.flip();
    out.write(buffer);
  }

  public long readLong(InputStream in) throws IOException
  {
    byte[] longBytes = new byte[Longs.BYTES];

    ByteStreams.readFully(in, longBytes);

    return Longs.fromByteArray(longBytes);
  }

  public void writeLongs(OutputStream out, long[] longs) throws IOException
  {
    writeInt(out, longs.length);

    for (long value : longs) {
      writeLong(out, value);
    }
  }

  public long[] readLongs(InputStream in) throws IOException
  {
    int size = readInt(in);

    long[] retVal = new long[size];
    for (int i = 0; i < size; ++i) {
      retVal[i] = readLong(in);
    }

    return retVal;
  }

  public void writeFloat(OutputStream out, float floatValue) throws IOException
  {
    writeInt(out, Float.floatToRawIntBits(floatValue));
  }

  public void writeFloat(WritableByteChannel out, float floatValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(Floats.BYTES);
    buffer.putFloat(floatValue);
    buffer.flip();
    out.write(buffer);
  }

  public float readFloat(InputStream in) throws IOException
  {
    return Float.intBitsToFloat(readInt(in));
  }

  public void writeFloats(OutputStream out, float[] floats) throws IOException
  {
    writeInt(out, floats.length);

    for (float value : floats) {
      writeFloat(out, value);
    }
  }

  public float[] readFloats(InputStream in) throws IOException
  {
    int size = readInt(in);

    float[] retVal = new float[size];
    for (int i = 0; i < retVal.length; ++i) {
      retVal[i] = readFloat(in);
    }

    return retVal;
  }

  public int getSerializedStringByteSize(String str)
  {
    return Ints.BYTES + StringUtils.toUtf8(str).length;
  }
}
