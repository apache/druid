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

package io.druid.segment.data;

import com.google.common.primitives.Ints;
import com.yahoo.memory.MemoryRegion;
import io.druid.java.util.common.io.smoosh.PositionalMemoryRegion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class MemorySerializer<T>
{
  public static <T> T read(PositionalMemoryRegion pMemory, ObjectStrategy<T> strategy)
  {
    int size = Integer.reverseBytes(pMemory.getInt());
    PositionalMemoryRegion memoryToUse = pMemory.duplicate();
    memoryToUse.limit(memoryToUse.position() + size);
    pMemory.position(memoryToUse.limit());

    return strategy.fromMemory(new MemoryRegion(pMemory.getRemainingMemory(), 0, size));
  }

  public static <T> void writeToChannel(T obj, ObjectStrategy<T> strategy, WritableByteChannel channel)
      throws IOException
  {
    byte[] toWrite = strategy.toBytes(obj);
    channel.write(ByteBuffer.allocate(Ints.BYTES).putInt(0, toWrite.length));
    channel.write(ByteBuffer.wrap(toWrite));
  }
}
