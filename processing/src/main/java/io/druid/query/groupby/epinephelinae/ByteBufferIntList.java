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

package io.druid.query.groupby.epinephelinae;

import com.google.common.primitives.Ints;
import com.metamx.common.IAE;
import io.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;

public class ByteBufferIntList
{
  private final ByteBuffer buffer;
  private final int maxElements;
  private int numElements;

  public ByteBufferIntList(
      ByteBuffer buffer,
      int maxElements
  )
  {
    this.buffer = buffer;
    this.maxElements = maxElements;
    this.numElements = 0;

    if (buffer.capacity() < (maxElements * Ints.BYTES)) {
      throw new IAE(
          "buffer for list is too small, was [%s] bytes, but need [%s] bytes.",
          buffer.capacity(),
          maxElements * Ints.BYTES
      );
    }
  }

  public void add(int val)
  {
    if (numElements == maxElements) {
      throw new IndexOutOfBoundsException(StringUtils.format("List is full with %d elements.", maxElements));
    }
    buffer.putInt(numElements * Ints.BYTES, val);
    numElements++;
  }

  public void set(int index, int val)
  {
    buffer.putInt(index * Ints.BYTES, val);
  }

  public int get(int index)
  {
    return buffer.getInt(index * Ints.BYTES);
  }

  public int getNumElements()
  {
    return numElements;
  }

  public void reset()
  {
    numElements = 0;
  }
}
