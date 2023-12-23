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

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Exists to provide an object-strategy. Bloom filters are query time only so does not fully implement this interface.
 */
public class BloomFilterSerde extends ComplexMetricSerde
{
  private static final BloomFilterObjectStrategy STRATEGY = new BloomFilterObjectStrategy();

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
    return STRATEGY;
  }

  private static class BloomFilterObjectStrategy implements ObjectStrategy<BloomKFilter>
  {
    @Override
    public Class<? extends BloomKFilter> getClazz()
    {
      return BloomKFilter.class;
    }

    @Nullable
    @Override
    public BloomKFilter fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      try {
        return BloomKFilter.deserialize(buffer, buffer.position());
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Nullable
    @Override
    public byte[] toBytes(@Nullable BloomKFilter val)
    {
      try {
        return BloomFilterSerializersModule.bloomKFilterToBytes(val);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int compare(BloomKFilter o1, BloomKFilter o2)
    {
      return BloomFilterAggregatorFactory.COMPARATOR.compare(o1, o2);
    }
  }
}
