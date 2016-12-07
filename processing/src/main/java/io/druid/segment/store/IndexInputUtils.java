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
package io.druid.segment.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * util class for IndexInput
 */
public class IndexInputUtils
{

  public static final int DEFAULT_BUFFER_SIZE = 512;

  /**
   * compare two IndexInput's remaining sequence of bytes lexicographically
   * the value returned keep consistent with Comparable.compareTo()
   * @param l
   * @param r
   * @return
   */
  public  static int compare(IndexInput l, IndexInput r){
    return -1;
  }


  /**
   * fetch the remaining size of the IndexInput
   *
   * @param indexInput
   *
   * @return
   */
  public static long remaining(IndexInput indexInput) throws IOException
  {
    /**
    long currentPositoin = indexInput.getFilePointer();
    long length = indexInput.length();
    return length - currentPositoin;
     */
    return indexInput.remaining();
  }

  /**
   * write the data from InedxInput to channel with default buffer size
   *
   * @param indexInput
   * @param channel
   *
   * @throws IOException
   */
  public static void write2Channel(IndexInput indexInput, WritableByteChannel channel) throws IOException
  {
    write2Channel(indexInput, channel, DEFAULT_BUFFER_SIZE);
  }

  /**
   * write the data from IndexInput to channel
   *
   * @param indexInput
   * @param channel
   * @param bufferSize default buffer size is 512 bytes
   *
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
      buffer.rewind();
    }
    int remainder = (int) (remainBytesSize % bufferSize);
    if (remainder != 0) {
      ByteBuffer remainderBuffer = ByteBuffer.allocate(remainder);
      byte[] remainderBytes = remainderBuffer.array();
      indexInput.readBytes(remainderBytes, 0, remainder);
      channel.write(remainderBuffer);
    }

  }

}
