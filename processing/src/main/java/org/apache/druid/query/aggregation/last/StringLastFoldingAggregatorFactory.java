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

package org.apache.druid.query.aggregation.last;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;

@JsonTypeName("stringLastFold")
public class StringLastFoldingAggregatorFactory extends StringLastAggregatorFactory
{
  public StringLastFoldingAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("maxStringBytes") Integer maxStringBytes
  )
  {
    super(name, fieldName, maxStringBytes);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory, BaseObjectColumnValueSelector selector)
  {
    return new StringLastAggregator(null, null, maxStringBytes)
    {
      @Override
      public void aggregate()
      {
        SerializablePairLongString pair = (SerializablePairLongString) selector.getObject();
        if (pair != null && pair.lhs >= lastTime) {
          lastTime = pair.lhs;
          lastValue = pair.rhs;
        }
      }
    };
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, BaseObjectColumnValueSelector selector)
  {
    return new StringLastBufferAggregator(null, null, maxStringBytes)
    {
      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        SerializablePairLongString pair = (SerializablePairLongString) selector.getObject();
        if (pair != null && pair.lhs != null) {
          ByteBuffer mutationBuffer = buf.duplicate();
          mutationBuffer.position(position);

          long lastTime = mutationBuffer.getLong(position);

          if (pair.lhs >= lastTime) {
            mutationBuffer.putLong(position, pair.lhs);
            if (pair.rhs != null) {
              byte[] valueBytes = StringUtils.toUtf8(pair.rhs);

              mutationBuffer.putInt(position + Long.BYTES, valueBytes.length);
              mutationBuffer.position(position + Long.BYTES + Integer.BYTES);
              mutationBuffer.put(valueBytes);
            } else {
              mutationBuffer.putInt(position + Long.BYTES, 0);
            }
          }
        }
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }
}
