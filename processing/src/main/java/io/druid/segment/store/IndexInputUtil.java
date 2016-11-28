package io.druid.segment.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * util class for IndexInput
 */
public class IndexInputUtil
{

  public static final int DEFAULT_BUFFER_SIZE = 512;
  /**
   * fetch the remaining size of the IndexInput
   *
   * @param indexInput
   *
   * @return
   */
  public static long remaining(IndexInput indexInput) throws IOException
  {
    long currentPositoin = indexInput.getFilePointer();
    long length = indexInput.length();
    return length - currentPositoin;
  }

  /**
   * write the data from InedxInput to channel with default buffer size
   * @param indexInput
   * @param channel
   * @throws IOException
   */
  public static void write2Channel(IndexInput indexInput, WritableByteChannel channel) throws IOException
  {
    write2Channel(indexInput,channel,DEFAULT_BUFFER_SIZE);
  }

  /**
   * write the data from IndexInput to channel
   * @param indexInput
   * @param channel
   * @param bufferSize default buffer size is 512 bytes
   * @throws IOException
   */
  public static void write2Channel(IndexInput indexInput, WritableByteChannel channel, int bufferSize)
      throws IOException
  {
    boolean hasRemaining = indexInput.hasRemaining();
    if (!hasRemaining) {
      return;
    }
    long remainBytesSize = remaining(indexInput);
    if (bufferSize <= 0) {
      bufferSize = DEFAULT_BUFFER_SIZE;
    }
    int times = (int) (remainBytesSize / bufferSize);
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    byte[] bufferBytes = buffer.array();
    for (int i = 0; i < times; i++) {
      indexInput.readBytes(bufferBytes, 0, bufferSize);
      channel.write(buffer);
    }
    int remainder = (int) (remainBytesSize % bufferSize);
    if(remainder!=0){
      ByteBuffer remainderBuffer = ByteBuffer.allocate(remainder);
      byte[] remainderBytes = remainderBuffer.array();
      indexInput.readBytes(remainderBytes,0,remainder);
      channel.write(remainderBuffer);
    }

  }

}
