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

package io.druid.query.aggregation;

import java.nio.ByteBuffer;

/**
 */
public class Aggregators
{
  public static Aggregator noopAggregator()
  {
    return new Aggregator()
    {
      @Override
      public void aggregate()
      {

      }

      @Override
      public void reset()
      {

      }

      @Override
      public Object get()
      {
        return null;
      }

      @Override
      public float getFloat()
      {
        return 0;
      }

      @Override
      public String getName()
      {
        return null;
      }

      @Override
      public void close()
      {

      }
    };
  }

  public static BufferAggregator noopBufferAggregator()
  {
    return new BufferAggregator()
    {
      @Override
      public void init(ByteBuffer buf, int position)
      {

      }

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {

      }

      @Override
      public Object get(ByteBuffer buf, int position)
      {
        return null;
      }

      @Override
      public float getFloat(ByteBuffer buf, int position)
      {
        return 0;
      }

      @Override
      public void close()
      {

      }
    };
  }
}
