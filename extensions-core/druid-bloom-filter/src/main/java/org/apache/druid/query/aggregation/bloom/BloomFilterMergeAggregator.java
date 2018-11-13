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

package org.apache.druid.query.aggregation.bloom;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class BloomFilterMergeAggregator implements Aggregator
{
  private final ColumnValueSelector<BloomKFilter> selector;
  private final BloomKFilter collector;

  public BloomFilterMergeAggregator(ColumnValueSelector<BloomKFilter> selector, int maxNumEntries)
  {
    this.selector = selector;
    this.collector = new BloomKFilter(maxNumEntries);
  }

  @Override
  public void aggregate()
  {
    BloomKFilter other = selector.getObject();
    if (other != null) {
      collector.merge(other);
    }
  }

  @Nullable
  @Override
  public Object get()
  {
    return collector;
  }


  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("BloomFilterMergeAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("BloomFilterMergeAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("BloomFilterMergeAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // nothing to close
  }
}
