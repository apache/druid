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

package io.druid.java.util.common;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 */
public class ByteBufferUtils
{
  /**
   * Releases memory held by the given direct ByteBuffer
   *
   * @param buffer buffer to free
   */
  public static void free(ByteBuffer buffer)
  {
    if (buffer.isDirect()) {
      clean((DirectBuffer) buffer);
    }
  }


  /**
   * Un-maps the given memory mapped file
   *
   * @param buffer buffer
   */
  public static void unmap(MappedByteBuffer buffer)
  {
    free(buffer);
  }

  private static void clean(DirectBuffer buffer)
  {
    final Cleaner cleaner = buffer.cleaner();
    if (cleaner != null) {
      cleaner.clean();
    }
  }
}
