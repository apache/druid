package com.metamx.druid.kv;

import com.google.common.primitives.Ints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class ByteBufferSerializer<T>
{
  public static <T> T read(ByteBuffer buffer, ObjectStrategy<T> strategy)
  {
    int size = buffer.getInt();
    ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
    bufferToUse.limit(bufferToUse.position() + size);
    buffer.position(bufferToUse.limit());

    return strategy.fromByteBuffer(bufferToUse, size);
  }

  public static <T> void writeToChannel(T obj, ObjectStrategy<T> strategy, WritableByteChannel channel)
      throws IOException
  {
    byte[] toWrite = strategy.toBytes(obj);
    channel.write(ByteBuffer.allocate(Ints.BYTES).putInt(0, toWrite.length));
    channel.write(ByteBuffer.wrap(toWrite));
  }
}
