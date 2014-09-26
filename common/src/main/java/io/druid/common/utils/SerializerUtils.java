/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.common.utils;

import com.google.common.io.ByteStreams;
import com.google.common.io.OutputSupplier;
import io.druid.collections.IntList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

public class SerializerUtils
{
  private static final Charset UTF8 = Charset.forName("UTF-8");

  public <T extends OutputStream> void writeString(T out, String name) throws IOException
  {
    byte[] nameBytes = name.getBytes(UTF8);
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
    byte[] nameBytes = name.getBytes(UTF8);
    writeInt(out, nameBytes.length);
    out.write(ByteBuffer.wrap(nameBytes));
  }

  public String readString(InputStream in) throws IOException
  {
    final int length = readInt(in);
    byte[] stringBytes = new byte[length];
    ByteStreams.readFully(in, stringBytes);
    return new String(stringBytes, UTF8);
  }

  public String readString(ByteBuffer in) throws IOException
  {
    final int length = in.getInt();
    byte[] stringBytes = new byte[length];
    in.get(stringBytes);
    return new String(stringBytes, UTF8);
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
    byte[] outBytes = new byte[4];

    ByteBuffer.wrap(outBytes).putInt(intValue);

    out.write(outBytes);
  }

  public void writeInt(WritableByteChannel out, int intValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(intValue);
    buffer.flip();
    out.write(buffer);
  }

  public int readInt(InputStream in) throws IOException
  {
    byte[] intBytes = new byte[4];

    ByteStreams.readFully(in, intBytes);

    return ByteBuffer.wrap(intBytes).getInt();
  }

  public void writeInts(OutputStream out, int[] ints) throws IOException
  {
    writeInt(out, ints.length);

    for (int i = 0; i < ints.length; i++) {
      writeInt(out, ints[i]);
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
    byte[] outBytes = new byte[8];

    ByteBuffer.wrap(outBytes).putLong(longValue);

    out.write(outBytes);
  }

  public void writeLong(WritableByteChannel out, long longValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(longValue);
    buffer.flip();
    out.write(buffer);
  }

  public long readLong(InputStream in) throws IOException
  {
    byte[] longBytes = new byte[8];

    ByteStreams.readFully(in, longBytes);

    return ByteBuffer.wrap(longBytes).getLong();
  }

  public void writeLongs(OutputStream out, long[] longs) throws IOException
  {
    writeInt(out, longs.length);

    for (int i = 0; i < longs.length; i++) {
      writeLong(out, longs[i]);
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

  public void writeFloat(OutputStream out, float intValue) throws IOException
  {
    byte[] outBytes = new byte[4];

    ByteBuffer.wrap(outBytes).putFloat(intValue);

    out.write(outBytes);
  }

  public void writeFloat(WritableByteChannel out, float floatValue) throws IOException
  {
    final ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putFloat(floatValue);
    buffer.flip();
    out.write(buffer);
  }

  public float readFloat(InputStream in) throws IOException
  {
    byte[] floatBytes = new byte[4];

    ByteStreams.readFully(in, floatBytes);

    return ByteBuffer.wrap(floatBytes).getFloat();
  }

  public void writeFloats(OutputStream out, float[] floats) throws IOException
  {
    writeInt(out, floats.length);

    for (int i = 0; i < floats.length; i++) {
      writeFloat(out, floats[i]);
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
}
