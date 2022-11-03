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

package org.apache.druid.segment.serde.cell;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * {@code StagedSerde} is useful when you have objects that have their own internal logic to serialize, but you wish to
 * compose the results of multiple serialized objects into a single ByteBuffer (or wrapped {@code byte[]}). Serializers
 * can implement {@code serializeDelayed} and return a {@code StorableBuffer}. This object allows the serialization to
 * be broken up so that serializers do whatever work is necessary to report how many bytes are needed. The caller can
 * then allocate a large enough byte[], wrap it in a ByteBuffer, and use the {@code StorableBuffer.store()} method.
 * <p/>
 * This results in superior efficiency over a {@code byte[] toBytes()} method because repeated copies of byte[] are
 * avoided.
 * <p/>
 * Since any serialization that returns a byte[] must reach a point in its serialization that it allocates
 * said byte[], that code may be executed to create the {@code StorableBuffer}. What code would have written to
 * a byte[] then makes calls to a ByteBuffer in the store() method.
 * <p/>
 * For the cases when it is not easy to break apart the serialization code, increased efficiency may be obtained by
 * overriding serialize() and directly returning bytes
 *
 * <p/>
 * example
 * <pre>{@code
 *   StagedSerde<Fuu> fuuSerde = new ...;
 *   StagedSerde<Bar> barerde = new ...;
 *   StorableBuffer fuuBuffer = fuuSerde.serializeDelayed(fuuInstance);
 *   StorableBuffer barBuffer = barSerde.serializeDelayed(barInstance);
 *   int size = fuuBuffer.getSerializedSize() + barBuffer.getSerializedSize();
 *   byte[] bytes = new byte[size];
 *   ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
 *
 *   fuuBuffer.store(buffer);
 *   barBuffer.store(buffer);
 *
 * }</pre>
 * <p/>
 * Note that for a common case in which you want a byte[] for a single object, a default implementation is provided
 * that does the above code for a single object.
 * <p/>
 *
 * @param <T>
 */
public interface StagedSerde<T>
{
  /**
   * Useful method when some computation is necessary to prepare for serialization without actually writing out
   * all the bytes in order to determine the serialized size. It allows encapsulation of the size computation and
   * the final logical to actually store into a ByteBuffer. It also allows for callers to pack multiple serialized
   * objects into a single ByteBuffer without extra copies of a byte[]/ByteBuffer by using the {@link StorableBuffer}
   * instance returned
   *
   * @param value - object to serialize
   * @return an object that reports its serialized size and how to serialize the object to a ByteBuffer
   */
  StorableBuffer serializeDelayed(@Nullable T value);

  /**
   * Default implementation for when a byte[] is desired. Typically, this default should suffice. Implementing
   * serializeDelayed() includes the logic of how to store into a ByteBuffer
   *
   * @param value - object to serialize
   * @return serialized byte[] of value
   */
  default byte[] serialize(T value)
  {
    StorableBuffer storableBuffer = serializeDelayed(value);
    ByteBuffer byteBuffer = ByteBuffer.allocate(storableBuffer.getSerializedSize()).order(ByteOrder.nativeOrder());

    storableBuffer.store(byteBuffer);

    return byteBuffer.array();
  }

  @Nullable
  T deserialize(ByteBuffer byteBuffer);

  default T deserialize(byte[] bytes)
  {
    return deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder()));
  }
}
