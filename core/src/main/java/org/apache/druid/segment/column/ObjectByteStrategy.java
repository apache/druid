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

package org.apache.druid.segment.column;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Naming is hard. This is the core interface extracted from another interface called ObjectStrategy that lives in
 * 'druid-processing'. It provides basic methods for handling converting some type of object to a binary form, reading
 * the binary form back into an object from a {@link ByteBuffer}, and mechanism to perform comparisons between objects.
 *
 * Complex types register one of these in {@link Types#registerStrategy}, which can be retrieved by the complex
 * type name to convert values to and from binary format, and compare them.
 *
 * This could be recombined with 'ObjectStrategy' should these two modules be combined.
 */
public interface ObjectByteStrategy<T> extends Comparator<T>
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
}
