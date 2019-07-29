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


import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

public class ObjectAccurateCardinalityAggregator extends BaseAccurateCardinalityAggregator<ColumnValueSelector>
{
  public ObjectAccurateCardinalityAggregator(
      ColumnValueSelector selector,
      LongBitmapCollectorFactory longBitmapCollectorFactory,
      boolean onHeap
  )
  {
    super(selector, longBitmapCollectorFactory, onHeap);
  }

  @Override
  void collectorAdd(LongBitmapCollector longBitmapCollector)
  {
    final Object object = selector.getObject();
    if (object == null) {
      return;
    }
    if (object instanceof LongBitmapCollector) {
      longBitmapCollector.fold((LongBitmapCollector) object);
    } else if (object instanceof Long) {
      longBitmapCollector.add(selector.getLong());
    } else {
      throw new IAE(
          "Cannot aggregat accurate cardinality for invalid column type"
      );
    }
  }
}
