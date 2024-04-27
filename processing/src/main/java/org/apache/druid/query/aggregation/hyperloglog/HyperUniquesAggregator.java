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

package org.apache.druid.query.aggregation.hyperloglog;

import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;

/**
 */
public class HyperUniquesAggregator implements Aggregator
{
  private final BaseObjectColumnValueSelector selector;

  private HyperLogLogCollector collector;

  public HyperUniquesAggregator(BaseObjectColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public synchronized void aggregate()
  {
    Object object = selector.getObject();
    if (object == null) {
      return;
    }
    if (collector == null) {
      collector = HyperLogLogCollector.makeLatestCollector();
    }
    collector.fold((HyperLogLogCollector) object);
  }

  @Nullable
  @Override
  public synchronized Object get()
  {
    if (collector == null) {
      return null;
    }
    // Must make a new collector duplicating the underlying buffer to ensure the object from "get" is usable
    // in a thread-safe manner.
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
  public void close()
  {
    // no resources to cleanup
  }
}
