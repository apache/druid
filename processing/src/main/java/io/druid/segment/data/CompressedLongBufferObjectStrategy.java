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
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
*/
public class CompressedLongBufferObjectStrategy extends CompressedObjectStrategy<LongBuffer>
{
  public static CompressedLongBufferObjectStrategy getBufferForOrder(ByteOrder order)
  {
    return new CompressedLongBufferObjectStrategy(order);
  }

  private CompressedLongBufferObjectStrategy(final ByteOrder order)
  {
    super(
        order,
        new BufferConverter<LongBuffer>()
        {
          @Override
          public LongBuffer convert(ByteBuffer buf)
          {
            return buf.asLongBuffer();
          }

          @Override
          public int compare(LongBuffer lhs, LongBuffer rhs)
          {
            return Ordering.natural().nullsFirst().compare(lhs, rhs);
          }

          @Override
          public int sizeOf(int count)
          {
            return count * Longs.BYTES;
          }

          @Override
          public LongBuffer combine(ByteBuffer into, LongBuffer from)
          {
            return into.asLongBuffer().put(from);
          }
        }
    );
  }

}
