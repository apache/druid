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

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface ObjectStrategy<T> extends Comparator<T>
{
  public Class<? extends T> getClazz();

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
  public T fromByteBuffer(ByteBuffer buffer, int numBytes);
  public byte[] toBytes(T val);
}
