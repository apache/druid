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

import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Default implementation of {@link TypeStrategy} for all {@link org.apache.druid.segment.serde.ComplexMetricSerde}
 * implementations that just wraps the {@link ObjectStrategy} they are required to implement.
 *
 * This is not likely to be the most efficient way to do things, especially since writing must first produce a byte
 * array before it can be written to the buffer, but it is cheap and should work correctly, which is important.
 */
public class ObjectStrategyComplexTypeStrategy<T> implements TypeStrategy<T>
{
  private final ObjectStrategy<T> objectStrategy;
  private final TypeSignature<?> typeSignature;

  public ObjectStrategyComplexTypeStrategy(ObjectStrategy<T> objectStrategy, TypeSignature<?> signature)
  {
    this.objectStrategy = objectStrategy;
    this.typeSignature = signature;
  }

  @Override
  public int estimateSizeBytes(@Nullable T value)
  {
    byte[] bytes = objectStrategy.toBytes(value);
    return bytes == null ? 0 : bytes.length;
  }

  @Override
  public T read(ByteBuffer buffer)
  {
    final int complexLength = buffer.getInt();
    ByteBuffer dupe = buffer.duplicate();
    dupe.limit(dupe.position() + complexLength);
    return objectStrategy.fromByteBuffer(dupe, complexLength);
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
  public int compare(T o1, T o2)
  {
    return objectStrategy.compare(o1, o2);
  }
}
