/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation;

import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.util.List;

public class JavaScriptAggregator implements Aggregator
{
  interface ScriptAggregator
  {
    double aggregate(double current, BaseObjectColumnValueSelector[] selectorList);

    double combine(double a, double b);

    double reset();

    void close();
  }

  private final BaseObjectColumnValueSelector[] selectorList;
  private final ScriptAggregator script;

  private double current;

  public JavaScriptAggregator(List<BaseObjectColumnValueSelector> selectorList, ScriptAggregator script)
  {
    this.selectorList = selectorList.toArray(new BaseObjectColumnValueSelector[0]);
    this.script = script;

    this.current = script.reset();
  }

  @Override
  public void aggregate()
  {
    current = script.aggregate(current, selectorList);
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
