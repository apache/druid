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

package org.apache.druid.query.aggregation.longunique;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.ArrayList;
import java.util.List;

public class LongUniqueBuildAggregator implements Aggregator
{
  private final ColumnValueSelector<Object> selector;
  private List<Roaring64NavigableMap> bitmaps;

  public LongUniqueBuildAggregator(ColumnValueSelector<Object> selector)
  {
    this.selector = selector;
    this.bitmaps = new ArrayList<>();
  }


  @Override
  public void aggregate()
  {
    final Object value = selector.getObject();
    if (value == null) {
      return;
    }
    bitmaps.add((Roaring64NavigableMap) value);
  }

  @Override
  public Roaring64NavigableMap get()
  {
    Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();
    if (bitmaps == null || bitmaps.isEmpty()) {
      return roaring64NavigableMap;
    }
    for (Roaring64NavigableMap navigableMap : bitmaps) {
      roaring64NavigableMap.or(navigableMap);
    }
    return roaring64NavigableMap;
  }

  @Override
  public float getFloat()
  {
    throw new UOE("Not implemented");
  }

  @Override
  public long getLong()
  {
    throw new UOE("Not implemented");
  }

  @Override
  public void close()
  {
    bitmaps = null;
  }
}
