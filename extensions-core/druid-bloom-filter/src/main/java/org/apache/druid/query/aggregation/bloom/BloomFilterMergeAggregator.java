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

import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.segment.ColumnValueSelector;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class BloomFilterMergeAggregator extends BaseBloomFilterAggregator<ColumnValueSelector<Object>>
{
  public BloomFilterMergeAggregator(ColumnValueSelector<Object> selector, BloomKFilter collector)
  {
    super(selector, collector);
  }

  @Override
  public void aggregate()
  {
    Object other = selector.getObject();
    if (other != null) {
      if (other instanceof BloomKFilter) {
        collector.merge((BloomKFilter) other);
      } else if (other instanceof ByteBuffer) {
        // fun fact: because bloom filter agg factory deserialize returns a byte buffer to avoid unnecessary serde,
        // but GroupByQueryEngine (group by v1) ends up trying to merge ByteBuffers from buffer aggs with this agg
        // instead of the BloomFilterBufferMergeAggregator. fun! Also, it requires a 'ComplexMetricSerde' to be
        // registered even for query time only aggs, but then never uses it. also fun!
        try {
          BloomKFilter otherFilter = BloomKFilter.deserialize((ByteBuffer) other);
          collector.merge(otherFilter);
        }
        catch (IOException ioe) {
          throw new RuntimeException("Failed to deserialize BloomKFilter", ioe);
        }
      }
    }
  }
}
