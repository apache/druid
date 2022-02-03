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

package org.apache.druid.query.aggregation.collectset;

import gnu.trove.set.hash.THashSet;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;


import java.util.Collection;
import java.util.Set;

public class CollectSetAggregator implements Aggregator
{
  private final ColumnValueSelector<Object> selector;
  private final int limit;
  private Set<Object> set;

  public CollectSetAggregator(
      ColumnValueSelector<Object> selector,
      int limit
  )
  {
    this.selector = selector;

    this.limit = limit;
    this.set = new THashSet<>();
  }

  @Override
  public void aggregate()
  {
    Object value = selector.getObject();
    if (value == null) {
      return;
    }

    if (limit >= 0 && set.size() >= limit) {
      return;
    }

    synchronized (this) {
      if (value instanceof Collection) {
        Collection<?> valueCollection = (Collection<?>) value;
        CollectSetUtil.addCollectionWithLimit(set, valueCollection, limit);
      } else {
        set.add(value);
      }
    }
  }

  @Override
  public synchronized Object get()
  {
    return new THashSet<>(set);
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    set = null;
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("Not implemented");
  }
}
