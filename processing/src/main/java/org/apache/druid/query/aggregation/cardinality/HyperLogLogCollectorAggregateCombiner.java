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

package org.apache.druid.query.aggregation.cardinality;

import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public final class HyperLogLogCollectorAggregateCombiner extends ObjectAggregateCombiner<HyperLogLogCollector>
{
  @Nullable
  private HyperLogLogCollector combined;

  @Override
  public void reset(ColumnValueSelector selector)
  {
    combined = null;
    fold(selector);
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    HyperLogLogCollector other = (HyperLogLogCollector) selector.getObject();
    if (other == null) {
      return;
    }
    if (combined == null) {
      combined = HyperLogLogCollector.makeLatestCollector();
    }
    combined.fold(other);
  }

  @Override
  public Class<HyperLogLogCollector> classOfObject()
  {
    return HyperLogLogCollector.class;
  }

  @Nullable
  @Override
  public HyperLogLogCollector getObject()
  {
    return combined;
  }
}
