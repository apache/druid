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

import java.nio.ByteBuffer;

/**
 * It's useful to return this object when multiple serializable objects are to be composed into a single ByteBuffer
 * or byte[]. This allows serializable objects to report their size so that callers may get the total size of all
 * objects and allocate a sufficiently large ByteBuffer/byte[]. Then the store() methods may be used to serialize.
 * <p/>
 * This avoids extra copies and wasted memory/gc pressure in the case of just returning a byte[].
 * <p/>
 * The getSerializedSize() method is provided because an object may need to use private data in order to calculate
 * the size needed.
 *
 **/
public interface StorableBuffer
{
  StorableBuffer EMPTY = new StorableBuffer()
  {
    @Override
    public void store(ByteBuffer byteBuffer)
    {
    }

    @Override
    public int getSerializedSize()
    {
      return 0;
    }
  };

  void store(ByteBuffer byteBuffer);

  int getSerializedSize();
}
