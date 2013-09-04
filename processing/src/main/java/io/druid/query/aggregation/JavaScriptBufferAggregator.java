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

import com.google.common.collect.Lists;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.List;

public class JavaScriptBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector[] selectorList;
  private final JavaScriptAggregator.ScriptAggregator script;

  public JavaScriptBufferAggregator(
      List<ObjectColumnSelector> selectorList,
      JavaScriptAggregator.ScriptAggregator script
  )
  {
    this.selectorList = Lists.newArrayList(selectorList).toArray(new ObjectColumnSelector[]{});
    this.script = script;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, script.reset());
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putDouble(position, script.aggregate(buf.getDouble(position), selectorList));
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float)buf.getDouble(position);
  }

  @Override
  public void close() {
    script.close();
  }
}
