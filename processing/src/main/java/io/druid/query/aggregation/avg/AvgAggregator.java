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

package io.druid.query.aggregation.avg;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;

/**
 */
public abstract class AvgAggregator implements Aggregator
{
  protected final AvgAggregatorCollector holder = new AvgAggregatorCollector();

  @Override
  public void reset()
  {
    holder.reset();
  }

  @Override
  public Object get()
  {
    return holder;
  }

  @Override
  public void close()
  {
  }

  @Override
  public float getFloat()
  {
    return (float) holder.compute();
  }

  @Override
  public long getLong()
  {
    return (long) holder.compute();
  }

  public static final class FloatAvgAggregator extends AvgAggregator
  {
    private final FloatColumnSelector selector;

    public FloatAvgAggregator(FloatColumnSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      holder.add(selector.get());
    }
  }

  public static final class LongAvgAggregator extends AvgAggregator
  {
    private final LongColumnSelector selector;

    public LongAvgAggregator(LongColumnSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      holder.add(selector.get());
    }
  }

  public static final class ObjectAvgAggregator extends AvgAggregator
  {
    private final ObjectColumnSelector selector;

    public ObjectAvgAggregator(ObjectColumnSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate()
    {
      AvgAggregatorCollector.combineValues(holder, selector.get());
    }
  }
}
