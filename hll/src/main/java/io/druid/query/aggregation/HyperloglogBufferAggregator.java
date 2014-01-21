/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package io.druid.query.aggregation;

import gnu.trove.map.hash.TIntByteHashMap;
import gnu.trove.procedure.TIntByteProcedure;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

public class HyperloglogBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;

  public HyperloglogBufferAggregator(ObjectColumnSelector selector)
  {
    this.selector = selector;
  }

  /*
   * byte 1 key length byte 2 value length byte 3...n key array byte n+1....
   * value array
   */
  @Override
  public void init(ByteBuffer buf, int position)
  {
    for (int i = 0; i < HyperloglogAggregator.m; i++) {
      buf.put(position + i, (byte) 0);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final ByteBuffer fb = buf;
    final int fp = position;
    final TIntByteHashMap newObj = (TIntByteHashMap) (selector.get());

    newObj.forEachEntry(
        new TIntByteProcedure()
        {
          public boolean execute(int a, byte b)
          {
            if (b > fb.get(fp + a)) {
              fb.put(fp + a, b);
            }
            return true;
          }
        }
    );
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    final TIntByteHashMap ret = new TIntByteHashMap();

    for (int i = 0; i < HyperloglogAggregator.m; i++) {
      if (buf.get(position + i) != 0) {
        ret.put(i, buf.get(position + i));
      }
    }
    return ret;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HyperloglogAggregator does not support getFloat()");
  }

  @Override
  public void close()
  {
    // do nothing
  }
}
