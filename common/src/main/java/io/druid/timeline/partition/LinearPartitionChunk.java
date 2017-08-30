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

package io.druid.timeline.partition;

public class LinearPartitionChunk <T> implements PartitionChunk<T>
{
  private final int chunkNumber;
  private final T object;

  public static <T> LinearPartitionChunk<T> make(int chunkNumber, T obj)
  {
    return new LinearPartitionChunk<T>(chunkNumber, obj);
  }

  public LinearPartitionChunk(
      int chunkNumber,
      T object
  )
  {
    this.chunkNumber = chunkNumber;
    this.object = object;
  }

  @Override
  public T getObject()
  {
    return object;
  }

  @Override
  public boolean abuts(PartitionChunk<T> chunk)
  {
    return true; // always complete
  }

  @Override
  public boolean isStart()
  {
    return true; // always complete
  }

  @Override

  public boolean isEnd()
  {
    return true; // always complete
  }

  @Override
  public int getChunkNumber()
  {
    return chunkNumber;
  }

  @Override
  public int compareTo(PartitionChunk<T> chunk)
  {
    if (chunk instanceof LinearPartitionChunk) {
      LinearPartitionChunk<T> linearChunk = (LinearPartitionChunk<T>) chunk;

      return Integer.compare(chunkNumber, linearChunk.chunkNumber);
    }
    throw new IllegalArgumentException("Cannot compare against something that is not a LinearPartitionChunk.");
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return compareTo((LinearPartitionChunk<T>) o) == 0;
  }

  @Override
  public int hashCode()
  {
    return chunkNumber;
  }
}
