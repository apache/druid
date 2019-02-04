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

import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class BloomFilterAggregateCombiner extends ObjectAggregateCombiner<BloomKFilter>
{
  @Nullable
  private BloomKFilter combined;

  private final int maxNumEntries;

  public BloomFilterAggregateCombiner(int maxNumEntries)
  {
    this.maxNumEntries = maxNumEntries;
  }

  @Override
  public void reset(ColumnValueSelector selector)
  {
    combined = null;
    fold(selector);
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    BloomKFilter other = (BloomKFilter) selector.getObject();
    if (other == null) {
      return;
    }
    if (combined == null) {
      combined = new BloomKFilter(maxNumEntries);
    }
    combined.merge(other);
  }

  @Nullable
  @Override
  public BloomKFilter getObject()
  {
    return combined;
  }

  @Override
  public Class<? extends BloomKFilter> classOfObject()
  {
    return BloomKFilter.class;
  }
}
