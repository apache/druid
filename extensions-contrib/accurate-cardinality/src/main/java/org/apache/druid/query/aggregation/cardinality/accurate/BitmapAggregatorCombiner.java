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

package org.apache.druid.query.aggregation.cardinality.accurate;

import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.Collector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.CollectorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.RoaringBitmapCollector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class BitmapAggregatorCombiner extends ObjectAggregateCombiner<Collector>
{

  @Nullable
  private Collector collector;
  private CollectorFactory collectorFactory;

  public BitmapAggregatorCombiner(
      CollectorFactory collectorFactory
  )
  {
    this.collectorFactory = collectorFactory;
  }

  @Override
  public void reset(ColumnValueSelector selector)
  {
    collector = null;
    fold(selector);
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    Object object = selector.getObject();
    if (object == null) {
      return;
    }
    if (collector == null) {
      collector = collectorFactory.makeEmptyCollector();
    }
    collector.fold((Collector) object);
  }

  @Nullable
  @Override
  public Collector getObject()
  {
    return collector;
  }

  @Override
  public Class<RoaringBitmapCollector> classOfObject()
  {
    return RoaringBitmapCollector.class;
  }
}
