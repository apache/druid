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

package org.apache.druid.segment.data;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

@ExtensionPoint
public interface ObjectStrategy<T> extends Comparator<T>
{
  Class<? extends T> getClazz();

  /**
   * Convert values from their underlying byte representation.
   *
   * Implementations of this method <i>may</i> change the given buffer's mark, or limit, and position.
   *
   * Implementations of this method <i>may not</i> store the given buffer in a field of the "deserialized" object,
   * need to use {@link ByteBuffer#slice()}, {@link ByteBuffer#asReadOnlyBuffer()} or {@link ByteBuffer#duplicate()} in
   * this case.
   *
   * @param buffer buffer to read value from
   * @param numBytes number of bytes used to store the value, starting at buffer.position()
   * @return an object created from the given byte buffer representation
   */
  @Nullable
  T fromByteBuffer(ByteBuffer buffer, int numBytes);

  @Nullable
  byte[] toBytes(@Nullable T val);

  /**
   * Reads 4-bytes numBytes from the given buffer, and then delegates to {@link #fromByteBuffer(ByteBuffer, int)}.
   */
  default T fromByteBufferWithSize(ByteBuffer buffer)
  {
    int size = buffer.getInt();
    ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
    bufferToUse.limit(bufferToUse.position() + size);
    buffer.position(bufferToUse.limit());

    return fromByteBuffer(bufferToUse, size);
  }

  default void writeTo(T val, WriteOutBytes out) throws IOException
  {
    byte[] bytes = toBytes(val);
    if (bytes != null) {
      out.write(bytes);
    }
  }
}
