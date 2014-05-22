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

import com.google.common.base.Charsets;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

/**
 * @author Hagen Rother, hagen@rother.cc
 */
public class SampleBufferAggregator implements BufferAggregator {
  private final ObjectColumnSelector selector;

  public SampleBufferAggregator(ObjectColumnSelector selector) {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position) {
    ByteBuffer duplicate = buf.duplicate();
    duplicate.position(position);
    duplicate.putShort((short) 0);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position) {
    ByteBuffer duplicate = buf.duplicate();
    duplicate.position(position);
    short size = duplicate.getShort();
    if (size == 0) {
      duplicate.position(position);
      byte[] raw = selector.get().toString().getBytes(Charsets.UTF_8);
      size = (short) Math.min(raw.length, 32766);
      duplicate.putShort(size);
      duplicate.put(raw, 0, size);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position) {
    ByteBuffer duplicate = buf.duplicate();
    duplicate.position(position);
    short size = duplicate.getShort();
    byte[] raw = new byte[size];
    duplicate.get(raw, 0, size);
    return new String(raw, Charsets.UTF_8);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position) {
    throw new UnsupportedOperationException("SampleBufferAggregator does not support getFloat()");
  }

  @Override
  public void close() {
  }
}
