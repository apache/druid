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

package org.apache.druid.utils;

import com.google.common.base.Preconditions;

/**
 * A circular buffer that supports random bidirectional access.
 *
 * @param <E> Type of object to be stored in the buffer
 */
public class CircularBuffer<E>
{
  public E[] getBuffer()
  {
    return buffer;
  }

  private final E[] buffer;

  private int start = 0;
  private int size = 0;

  public CircularBuffer(int capacity)
  {
    Preconditions.checkArgument(capacity > 0, "Capacity must be greater than 0.");
    buffer = (E[]) new Object[capacity];
  }

  public void add(E item)
  {
    buffer[start++] = item;

    if (start >= buffer.length) {
      start = 0;
    }

    if (size < buffer.length) {
      size++;
    }
  }

  /**
   * Access object at a given index, starting from the latest entry added and moving backwards.
   */
  public E getLatest(int index)
  {
    Preconditions.checkArgument(index >= 0 && index < size, "invalid index");

    int bufferIndex = start - index - 1;
    if (bufferIndex < 0) {
      bufferIndex = buffer.length + bufferIndex;
    }
    return buffer[bufferIndex];
  }

  /**
   * Access object at a given index, starting from the earliest entry added and moving forward.
   */
  public E get(int index)
  {
    Preconditions.checkArgument(index >= 0 && index < size, "invalid index");

    int bufferIndex = (start - size + index) % buffer.length;
    if (bufferIndex < 0) {
      bufferIndex += buffer.length;
    }
    return buffer[bufferIndex];
  }

  public int size()
  {
    return size;
  }
}
