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

package org.apache.druid.query.movingaverage.averagers;

import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ObjectColumnSelector;

/**
 * This `default` type averager will use the same aggregator of its dependent aggregation field to do trailing aggregate.
 */
public class DefaultAggregateAverager extends BaseAverager<Object, Object>
{

  private final SettableObjectColumnValueSelector selector = new SettableObjectColumnValueSelector();
  private final AggregatorFactory agg;

  public DefaultAggregateAverager(
      Class<Object> storageType,
      int numBuckets,
      String name,
      String fieldName,
      int cycleSize,
      AggregatorFactory agg
  )
  {
    super(storageType, numBuckets, name, fieldName, cycleSize);
    this.agg = agg;
  }

  @Override
  public boolean finalizeMetric()
  {
    // we don't finalize metrics when adding them to the buckets to ensure aggregating complex metrics correctly
    return false;
  }

  @Override
  protected Object computeResult()
  {
    boolean reset = true;
    AggregateCombiner combiner = agg.makeAggregateCombiner();
    for (int i = 0; i < numBuckets; i += cycleSize) {
      int index = (i + startFrom) % numBuckets;
      if (buckets[index] != null) {
        selector.setObject(buckets[index]);
        if (reset) {
          combiner.reset(selector);
          reset = false;
        } else {
          combiner.fold(selector);
        }
      }
    }
    startFrom++;
    return agg.finalizeComputation(combiner.getObject());
  }

  static class SettableObjectColumnValueSelector extends ObjectColumnSelector<Object>
  {

    private Object object;

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Override
    public Object getObject()
    {
      return object;
    }

    public void setObject(Object object)
    {
      this.object = object;
    }

    @Override
    public Class<?> classOfObject()
    {
      return Object.class;
    }
  }
}
