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

package org.apache.druid.query.aggregation.histogram;

import com.google.common.collect.Ordering;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class FixedBucketsHistogramSerde extends ComplexMetricSerde
{
  private static final Logger LOG = new Logger(FixedBucketsHistogramSerde.class);

  private static Ordering<FixedBucketsHistogram> comparator = new Ordering<FixedBucketsHistogram>()
  {
    @Override
    public int compare(
        FixedBucketsHistogram arg1,
        FixedBucketsHistogram arg2
    )
    {
      return FixedBucketsHistogramAggregator.COMPARATOR.compare(arg1, arg2);
    }
  }.nullsFirst();

  @Override
  public String getTypeName()
  {
    return FixedBucketsHistogramAggregator.TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<FixedBucketsHistogram> extractedClass()
      {
        return FixedBucketsHistogram.class;
      }

      @Nullable
      @Override
      public Object extractValue(InputRow inputRow, String metricName)
      {
        throw new UnsupportedOperationException("extractValue without an aggregator factory is not supported.");
      }

      @Override
      public FixedBucketsHistogram extractValue(InputRow inputRow, String metricName, AggregatorFactory agg)
      {
        Object rawValue = inputRow.getRaw(metricName);

        FixedBucketsHistogramAggregatorFactory aggregatorFactory = (FixedBucketsHistogramAggregatorFactory) agg;

        if (rawValue == null) {
          FixedBucketsHistogram fbh = new FixedBucketsHistogram(
              aggregatorFactory.getLowerLimit(),
              aggregatorFactory.getUpperLimit(),
              aggregatorFactory.getNumBuckets(),
              aggregatorFactory.getOutlierHandlingMode()
          );
          if (NullHandling.replaceWithDefault()) {
            fbh.add(NullHandling.defaultDoubleValue());
          } else {
            fbh.incrementMissing();
          }
          return fbh;
        } else if (rawValue instanceof Number) {
          FixedBucketsHistogram fbh = new FixedBucketsHistogram(
              aggregatorFactory.getLowerLimit(),
              aggregatorFactory.getUpperLimit(),
              aggregatorFactory.getNumBuckets(),
              aggregatorFactory.getOutlierHandlingMode()
          );
          fbh.add(((Number) rawValue).doubleValue());
          return fbh;
        } else if (rawValue instanceof FixedBucketsHistogram) {
          return (FixedBucketsHistogram) rawValue;
        } else if (rawValue instanceof String) {
          Number numberAttempt;
          try {
            numberAttempt = Rows.objectToNumber(metricName, rawValue);
            FixedBucketsHistogram fbh = new FixedBucketsHistogram(
                aggregatorFactory.getLowerLimit(),
                aggregatorFactory.getUpperLimit(),
                aggregatorFactory.getNumBuckets(),
                aggregatorFactory.getOutlierHandlingMode()
            );
            fbh.add(numberAttempt.doubleValue());
            return fbh;
          }
          catch (ParseException pe) {
            FixedBucketsHistogram fbh = FixedBucketsHistogram.fromBase64((String) rawValue);
            return fbh;
          }
        } else {
          throw new UnsupportedOperationException("Unknown type: " + rawValue.getClass());
        }
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    final GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy(), builder.getFileMapper());
    builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<FixedBucketsHistogram>()
    {
      @Override
      public Class<? extends FixedBucketsHistogram> getClazz()
      {
        return FixedBucketsHistogram.class;
      }

      @Override
      public FixedBucketsHistogram fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        buffer.limit(buffer.position() + numBytes);
        FixedBucketsHistogram fbh = FixedBucketsHistogram.fromByteBuffer(buffer);
        return fbh;
      }

      @Override
      public byte[] toBytes(FixedBucketsHistogram h)
      {
        if (h == null) {
          return new byte[]{};
        }

        return h.toBytes();
      }

      @Override
      public int compare(FixedBucketsHistogram o1, FixedBucketsHistogram o2)
      {
        return comparator.compare(o1, o2);
      }
    };
  }

  @Override
  public GenericColumnSerializer getSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String column
  )
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }
}
