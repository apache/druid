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

package io.druid.query.aggregation.hyperloglog;

import io.druid.hll.HyperLogLogCollector;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

/**
 */
public class HyperUniquesAggregator implements Aggregator
{
  private final ObjectColumnSelector selector;

  private HyperLogLogCollector collector;

  public HyperUniquesAggregator(
      ObjectColumnSelector selector
  )
  {
    this.selector = selector;

    this.collector = HyperLogLogCollector.makeLatestCollector();
  }

  @Override
  public void aggregate()
  {
    collector.fold((HyperLogLogCollector) selector.get());
  }

  @Override
  public void reset()
  {
    collector = HyperLogLogCollector.makeLatestCollector();
  }

  @Override
  public Object get()
  {
    // Workaround for non-thread-safe use of HyperLogLogCollector.
    // OnheapIncrementalIndex has a penchant for calling "aggregate" and "get" simultaneously.
    return HyperLogLogCollector.makeCollectorSharingStorage(collector);
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("HyperUniquesAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("HyperUniquesAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("HyperUniquesAggregator does not support getDouble()");
  }

  @Override
  public Aggregator clone()
  {
    return new HyperUniquesAggregator(selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
