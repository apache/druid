/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.common.utils;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.error.DruidException;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.serde.Serializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

public class SerializerUtils
{
  private static final EnumSet<StandardOpenOption> MAP_SERIALIZER_OPTIONS = EnumSet.of(
      StandardOpenOption.READ,
      StandardOpenOption.WRITE,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
  );

  public <T extends OutputStream> void writeString(T out, String name) throws IOException
  {
    byte[] nameBytes = StringUtils.toUtf8(name);
    writeInt(out, nameBytes.length);
    out.write(nameBytes);
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

  public String readString(ByteBuffer in)
  {
    final int length = in.getInt();
    return StringUtils.fromUtf8(readBytes(in, length));
  }

  public byte[] readBytes(ByteBuffer in, int length)
  {
    byte[] bytes = new byte[length];
    in.get(bytes);
    return bytes;
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

  String[] readStrings(ByteBuffer in)
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


  /**
   * Write a {@link Serializer} to a {@link Path} and then map it as a read-only {@link MappedByteBuffer}
   */
  @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
  public static MappedByteBuffer mapSerializer(Path path, Serializer writer)
  {
    try (final FileChannel fileChannel = FileChannel.open(path, MAP_SERIALIZER_OPTIONS);
         final GatheringByteChannel smooshChannel = createSerializableChannel(fileChannel, writer.getSerializedSize())) {
      //noinspection DataFlowIssue
      writer.writeTo(smooshChannel, null);
      return fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, writer.getSerializedSize());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a {@link GatheringByteChannel} from another {@link FileChannel} with size set correctly so it is suitable
   * to use for writing with a {@link Serializer}
   */
  private static GatheringByteChannel createSerializableChannel(FileChannel channel, long size)
  {
    // this is pretty similar to some implementations of SmooshedWriter in FileSmoosher, some of which could probably
    // be refactored to use this method in the future.
    return new GatheringByteChannel()
    {
      private boolean isClosed = false;
      private long currOffset = 0;

      @Override
      public boolean isOpen()
      {
        return !isClosed;
      }

      @Override
      public void close() throws IOException
      {
        channel.close();
        isClosed = true;
      }

      @Override
      public int write(ByteBuffer buffer) throws IOException
      {
        return addToOffset(channel.write(buffer));
      }

      @Override
      public long write(ByteBuffer[] srcs, int offset, int length) throws IOException
      {
        return addToOffset(channel.write(srcs, offset, length));
      }

      @Override
      public long write(ByteBuffer[] srcs) throws IOException
      {
        return addToOffset(channel.write(srcs));
      }

      public int addToOffset(long numBytesWritten)
      {
        final long bytesLeft = size - currOffset;
        if (numBytesWritten > bytesLeft) {
          throw DruidException.defensive(
              "Wrote more bytes[%,d] than available[%,d]. Don't do that.",
              numBytesWritten,
              bytesLeft
          );
        }
        currOffset += numBytesWritten;

        return Ints.checkedCast(numBytesWritten);
      }
    };
  }
}
