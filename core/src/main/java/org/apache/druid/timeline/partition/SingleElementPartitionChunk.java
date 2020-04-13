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

package org.apache.druid.timeline.partition;

/**
 */
public class SingleElementPartitionChunk<T> implements PartitionChunk<T>
{
  private final T element;

  public SingleElementPartitionChunk(T element)
  {
    this.element = element;
  }

  @Override
  public T getObject()
  {
    return element;
  }

  @Override
  public boolean abuts(PartitionChunk<T> tPartitionChunk)
  {
    return false;
  }

  @Override
  public boolean isStart()
  {
    return true;
  }

  @Override
  public boolean isEnd()
  {
    return true;
  }

  @Override
  public int getChunkNumber()
  {
    return 0;
  }

  /**
   * The ordering of PartitionChunks is determined entirely by the partition boundaries and has nothing to do
   * with the object.  Thus, if there are two SingleElementPartitionChunks, they are equal because they both
   * represent the full partition space.
   *
   * SingleElementPartitionChunks are currently defined as less than every other type of PartitionChunk.  There
   * is no good reason for it, nor is there a bad reason, that's just the way it is.  This is subject to change.
   *
   * @param chunk
   * @return
   */
  @Override
  public int compareTo(PartitionChunk<T> chunk)
  {
    return chunk instanceof SingleElementPartitionChunk ? 0 : -1;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "SingleElementPartitionChunk{" +
           "element=" + element +
           '}';
  }
}
