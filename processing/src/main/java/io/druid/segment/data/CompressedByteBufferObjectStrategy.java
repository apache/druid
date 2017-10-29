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

import io.druid.java.util.common.guava.Comparators;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CompressedByteBufferObjectStrategy extends FixedSizeCompressedObjectStrategy<ByteBuffer>
{

  public static CompressedByteBufferObjectStrategy getBufferForOrder(final ByteOrder order, final CompressionStrategy compression, final int sizePer)
  {
    return new CompressedByteBufferObjectStrategy(order, compression, sizePer);
  }

  public CompressedByteBufferObjectStrategy(
      ByteOrder order,
      CompressionStrategy compression,
      final int sizePer
  )
  {
    super(
        order, new BufferConverter<ByteBuffer>()
        {
          @Override
          public ByteBuffer convert(ByteBuffer buf)
          {
            return buf;
          }

          @Override
          public int compare(ByteBuffer lhs, ByteBuffer rhs)
          {
            return Comparators.<ByteBuffer>naturalNullsFirst().compare(lhs, rhs);
          }

          @Override
          public int sizeOf(int count)
          {
            return count; // 1 byte per element
          }

          @Override
          public ByteBuffer combine(ByteBuffer into, ByteBuffer from)
          {
            return into.put(from);
          }
        }, compression, sizePer
    );
  }
}
