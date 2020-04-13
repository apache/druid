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

import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;

/**
 * Dummy {@link ComplexMetricSerde} that exists so {@link BloomFilterAggregatorFactory} has something to register so
 * {@link org.apache.druid.query.groupby.GroupByQueryEngine} will work, but isn't actually used because bloom filter
 * aggregators are currently only implemented for use at query time
 */
public class BloomFilterSerde extends ComplexMetricSerde
{
  @Override
  public String getTypeName()
  {
    return BloomFilterSerializersModule.BLOOM_FILTER_TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    throw new UnsupportedOperationException("Bloom filter aggregators are query-time only");
  }

  @Override
  public void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder)
  {
    throw new UnsupportedOperationException("Bloom filter aggregators are query-time only");
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    throw new UnsupportedOperationException("Bloom filter aggregators are query-time only");
  }

  @Override
  public ObjectStrategy<BloomKFilter> getObjectStrategy()
  {
    throw new UnsupportedOperationException("Bloom filter aggregators are query-time only");
  }
}
