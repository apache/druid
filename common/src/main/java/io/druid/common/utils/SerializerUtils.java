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
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.io.Channels;
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
    out.write(helperBuffer.array(), helperBuffer.arrayOffset(), Integer.BYTES);
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
    Channels.writeFully(out, ByteBuffer.wrap(nameBytes));
  }

  String readString(InputStream in) throws IOException
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

  void writeStrings(OutputStream out, String[] names) throws IOException
  {
    writeStrings(out, Arrays.asList(names));
  }

  private void writeStrings(OutputStream out, List<String> names) throws IOException
  {
    writeInt(out, names.size());

    for (String name : names) {
      writeString(out, name);
    }
  }

  String[] readStrings(InputStream in) throws IOException
  {
    int length = readInt(in);

    String[] retVal = new String[length];

    for (int i = 0; i < length; ++i) {
      retVal[i] = readString(in);
    }

    return retVal;
  }

  String[] readStrings(ByteBuffer in) throws IOException
  {
    int length = in.getInt();

    String[] retVal = new String[length];

    for (int i = 0; i < length; ++i) {
      retVal[i] = readString(in);
    }

    return retVal;
  }

  private void writeInt(OutputStream out, int intValue) throws IOException
  {
    out.write(Ints.toByteArray(intValue));
  }

  public static void writeInt(WritableByteChannel out, int intValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.putInt(intValue);
    buffer.flip();
    Channels.writeFully(out, buffer);
  }

  private int readInt(InputStream in) throws IOException
  {
    byte[] intBytes = new byte[Integer.BYTES];

    ByteStreams.readFully(in, intBytes);

    return Ints.fromByteArray(intBytes);
  }

  void writeInts(OutputStream out, int[] ints) throws IOException
  {
    writeInt(out, ints.length);

    for (int value : ints) {
      writeInt(out, value);
    }
  }

  int[] readInts(InputStream in) throws IOException
  {
    int size = readInt(in);

    int[] retVal = new int[size];
    for (int i = 0; i < size; ++i) {
      retVal[i] = readInt(in);
    }

    return retVal;
  }

  private void writeLong(OutputStream out, long longValue) throws IOException
  {
    out.write(Longs.toByteArray(longValue));
  }

  public void writeLong(WritableByteChannel out, long longValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(longValue);
    buffer.flip();
    Channels.writeFully(out, buffer);
  }

  long readLong(InputStream in) throws IOException
  {
    byte[] longBytes = new byte[Long.BYTES];

    ByteStreams.readFully(in, longBytes);

    return Longs.fromByteArray(longBytes);
  }

  void writeLongs(OutputStream out, long[] longs) throws IOException
  {
    writeInt(out, longs.length);

    for (long value : longs) {
      writeLong(out, value);
    }
  }

  long[] readLongs(InputStream in) throws IOException
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

  void writeFloat(WritableByteChannel out, float floatValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES);
    buffer.putFloat(floatValue);
    buffer.flip();
    Channels.writeFully(out, buffer);
  }

  float readFloat(InputStream in) throws IOException
  {
    return Float.intBitsToFloat(readInt(in));
  }

  void writeFloats(OutputStream out, float[] floats) throws IOException
  {
    writeInt(out, floats.length);

    for (float value : floats) {
      writeFloat(out, value);
    }
  }

  float[] readFloats(InputStream in) throws IOException
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
    return Integer.BYTES + StringUtils.toUtf8(str).length;
  }
}
