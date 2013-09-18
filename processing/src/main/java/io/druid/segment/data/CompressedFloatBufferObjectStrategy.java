/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Floats;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

/**
*/
public class CompressedFloatBufferObjectStrategy extends CompressedObjectStrategy<FloatBuffer>
{
  public static CompressedFloatBufferObjectStrategy getBufferForOrder(ByteOrder order)
  {
    return new CompressedFloatBufferObjectStrategy(order);
  }

  private CompressedFloatBufferObjectStrategy(final ByteOrder order)
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
            return Ordering.natural().nullsFirst().compare(lhs, rhs);
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
        }
    );
  }
}
