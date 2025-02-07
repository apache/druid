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

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;
import java.util.Collection;

public class ApproximateHistogramFoldingSerde extends ComplexMetricSerde
{

  public static final String TYPE_NAME = "approximateHistogram";

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<ApproximateHistogram> extractedClass()
      {
        return ApproximateHistogram.class;
      }

      @Override
      public ApproximateHistogram extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);

        if (rawValue == null) {
          return new ApproximateHistogram(0);
        } else if (rawValue instanceof ApproximateHistogram) {
          return (ApproximateHistogram) rawValue;
        } else {
          ApproximateHistogram h = new ApproximateHistogram();

          if (rawValue instanceof Collection) {
            for (final Object next : ((Collection) rawValue)) {
              if (next != null) {
                h.offer(Rows.objectToNumber(metricName, next, true).floatValue());
              }
            }
          } else {
            h.offer(Rows.objectToNumber(metricName, rawValue, true).floatValue());
          }

          return h;
        }
      }
    };
  }

  @Override
  public ObjectStrategy<ApproximateHistogram> getObjectStrategy()
  {
    return new ObjectStrategy<>()
    {
      @Override
      public Class<ApproximateHistogram> getClazz()
      {
        return ApproximateHistogram.class;
      }

      @Override
      public ApproximateHistogram fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        buffer.limit(buffer.position() + numBytes);
        return ApproximateHistogram.fromBytes(buffer);
      }

      @Override
      public byte[] toBytes(ApproximateHistogram h)
      {
        if (h == null) {
          return ByteArrays.EMPTY_ARRAY;
        }
        return h.toBytes();
      }

      @Override
      public int compare(ApproximateHistogram o1, ApproximateHistogram o2)
      {
        return ApproximateHistogramAggregator.COMPARATOR.compare(o1, o2);
      }

      @Override
      public boolean readRetainsBufferReference()
      {
        return false;
      }
    };
  }
}
