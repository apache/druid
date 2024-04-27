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

import it.unimi.dsi.fastutil.Hash;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Default implementation of {@link TypeStrategy} for all {@link org.apache.druid.segment.serde.ComplexMetricSerde}
 * implementations that just wraps the {@link ObjectStrategy} they are required to implement.
 * <p>
 * This is not likely to be the most efficient way to do things, especially since writing must first produce a byte
 * array before it can be written to the buffer, but it is cheap and should work correctly, which is important.
 */
public class ObjectStrategyComplexTypeStrategy<T> implements TypeStrategy<T>
{
  private final ObjectStrategy<T> objectStrategy;
  private final TypeSignature<?> typeSignature;
  @Nullable
  private final Hash.Strategy<T> hashStrategy;

  public ObjectStrategyComplexTypeStrategy(ObjectStrategy<T> objectStrategy, TypeSignature<?> signature)
  {
    this(objectStrategy, signature, null);
  }

  public ObjectStrategyComplexTypeStrategy(
      ObjectStrategy<T> objectStrategy,
      TypeSignature<?> signature,
      @Nullable final Hash.Strategy<T> hashStrategy
  )
  {
    this.objectStrategy = objectStrategy;
    this.typeSignature = signature;
    this.hashStrategy = hashStrategy;

  }

  @Override
  public int estimateSizeBytes(@Nullable T value)
  {
    byte[] bytes = objectStrategy.toBytes(value);
    return Integer.BYTES + (bytes == null ? 0 : bytes.length);
  }

  @Override
  public T read(ByteBuffer buffer)
  {
    final int complexLength = buffer.getInt();
    ByteBuffer dupe = buffer.duplicate();
    dupe.order(buffer.order());
    dupe.limit(dupe.position() + complexLength);
    T value = objectStrategy.fromByteBuffer(dupe, complexLength);
    buffer.position(buffer.position() + complexLength);
    return value;
  }

  @Override
  public boolean readRetainsBufferReference()
  {
    // Can't guarantee that ObjectStrategy *doesn't* retain a reference.
    return true;
  }

  @Override
  public int write(ByteBuffer buffer, T value, int maxSizeBytes)
  {
    TypeStrategies.checkMaxSize(buffer.remaining(), maxSizeBytes, typeSignature);
    byte[] bytes = objectStrategy.toBytes(value);
    final int sizeBytes = Integer.BYTES + bytes.length;
    final int remaining = maxSizeBytes - sizeBytes;
    if (remaining >= 0) {
      buffer.putInt(bytes.length);
      buffer.put(bytes, 0, bytes.length);
      return sizeBytes;
    }
    return remaining;
  }

  @Override
  public int compare(Object o1, Object o2)
  {
    return objectStrategy.compare((T) o1, (T) o2);
  }

  @Override
  public T fromBytes(byte[] value)
  {
    return objectStrategy.fromByteBufferSafe(ByteBuffer.wrap(value), value.length);
  }

  @Override
  public boolean groupable()
  {
    return hashStrategy != null;
  }

  @Override
  public int hashCode(T o)
  {
    if (hashStrategy == null) {
      throw DruidException.defensive("hashStrategy not provided");
    }
    return hashStrategy.hashCode(o);
  }

  @Override
  public boolean equals(T a, T b)
  {
    if (hashStrategy == null) {
      throw DruidException.defensive("hashStrategy not provided");
    }
    return hashStrategy.equals(a, b);
  }
}
