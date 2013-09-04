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

import java.util.List;

public class JavaScriptAggregator implements Aggregator
{
  static interface ScriptAggregator
  {
    public double aggregate(double current, ObjectColumnSelector[] selectorList);

    public double combine(double a, double b);

    public double reset();

    public void close();
  }

  private final String name;
  private final ObjectColumnSelector[] selectorList;
  private final ScriptAggregator script;

  private volatile double current;

  public JavaScriptAggregator(String name, List<ObjectColumnSelector> selectorList, ScriptAggregator script)
  {
    this.name = name;
    this.selectorList = Lists.newArrayList(selectorList).toArray(new ObjectColumnSelector[]{});
    this.script = script;

    this.current = script.reset();
  }

  @Override
  public void aggregate()
  {
    current = script.aggregate(current, selectorList);
  }

  @Override
  public void reset()
  {
    current = script.reset();
  }

  @Override
  public Object get()
  {
    return current;
  }

  @Override
  public float getFloat()
  {
    return (float) current;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {
    script.close();
  }
}
