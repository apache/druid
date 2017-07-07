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

  private final ObjectColumnSelector[] selectorList;
  private final ScriptAggregator script;

  private double current;

  public JavaScriptAggregator(List<ObjectColumnSelector> selectorList, ScriptAggregator script)
  {
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
  public long getLong()
  {
    return (long) current;
  }

  @Override
  public double getDouble()
  {
    return current;
  }

  @Override
  public void close()
  {
    script.close();
  }
}
