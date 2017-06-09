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

import com.google.common.primitives.Floats;
import io.druid.java.util.common.guava.Comparators;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

/**
*/
public class CompressedFloatBufferObjectStrategy extends FixedSizeCompressedObjectStrategy<FloatBuffer>
{
  public static CompressedFloatBufferObjectStrategy getBufferForOrder(final ByteOrder order, final CompressionStrategy compression, final int sizePer)
  {
    return new CompressedFloatBufferObjectStrategy(order, compression, sizePer);
  }

  private CompressedFloatBufferObjectStrategy(final ByteOrder order, final CompressionStrategy compression, final int sizePer)
  {
    super(
        order,
        new BufferConverter<FloatBuffer>()
        {
          @Override
          public FloatBuffer convert(ByteBuffer buf)
          {
            return buf.asFloatBuffer();
          }

          @Override
          public int compare(FloatBuffer lhs, FloatBuffer rhs)
          {
            return Comparators.<FloatBuffer>naturalNullsFirst().compare(lhs, rhs);
          }

          @Override
          public int sizeOf(int count)
          {
            return count * Floats.BYTES;
          }

          @Override
          public FloatBuffer combine(ByteBuffer into, FloatBuffer from)
          {
            return into.asFloatBuffer().put(from);
          }
        },
        compression,
        sizePer
    );
  }
}
