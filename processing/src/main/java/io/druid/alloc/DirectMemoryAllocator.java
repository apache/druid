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

package io.druid.alloc;

import io.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;

public final class DirectMemoryAllocator
{
  private static final Logger log = new Logger(DirectMemoryAllocator.class);
  private static final int PAGE_SIZE = 4096;

  /**
   * Ensures that all pages of the BB are physically allocated on Linux, which has lazy physical allocation by default.
   * It might help with memory fragmentation issues, and Linux OOM crashes.
   */
  private static final boolean preTouch = Boolean.getBoolean("preTouchDirectMemory");

  public static ByteBuffer allocate(int size, String reason)
  {
    log.info("Allocating direct memory for [%s] of [%,d] bytes, pre touch: [%s]", reason, size, preTouch);
    ByteBuffer result = ByteBuffer.allocateDirect(size);
    if (preTouch) {
      touch(result);
    }
    return result;
  }

  private static void touch(ByteBuffer buffer)
  {
    if (buffer.capacity() == 0) {
      return;
    }
    for (int i = 0; i < buffer.capacity(); i += PAGE_SIZE) {
      buffer.put(i, (byte) 0);
    }
    buffer.put(buffer.capacity() - 1, (byte) 0);
  }

  private DirectMemoryAllocator() {}
}
