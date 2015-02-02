/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

import io.druid.query.filter.ValueMatcher;

import java.nio.ByteBuffer;

public class FilteredBufferAggregator implements BufferAggregator
{
  private final ValueMatcher matcher;
  private final BufferAggregator delegate;

  public FilteredBufferAggregator(ValueMatcher matcher, BufferAggregator delegate)
  {
    this.matcher = matcher;
    this.delegate = delegate;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    delegate.init(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (matcher.matches()) {
      delegate.aggregate(buf, position);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return delegate.get(buf, position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return delegate.getLong(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return delegate.getFloat(buf, position);
  }

  @Override
  public void close()
  {
    delegate.close();
  }
}
