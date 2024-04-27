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

package org.apache.druid.spectator.histogram;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;

public class SpectatorHistogramComplexMetricSerde extends ComplexMetricSerde
{
  private static final SpectatorHistogramObjectStrategy STRATEGY = new SpectatorHistogramObjectStrategy();
  private final String typeName;

  SpectatorHistogramComplexMetricSerde(String type)
  {
    this.typeName = type;
  }

  @Override
  public String getTypeName()
  {
    return typeName;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<SpectatorHistogram> extractedClass()
      {
        return SpectatorHistogram.class;
      }

      @Override
      public Object extractValue(final InputRow inputRow, final String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object == null || object instanceof SpectatorHistogram || object instanceof Number) {
          return object;
        }
        if (object instanceof String) {
          String objectString = (String) object;
          // Ignore empty values
          if (objectString.trim().isEmpty()) {
            return null;
          }
          // Treat as long number, if it looks like a number
          if (Character.isDigit((objectString).charAt(0))) {
            return Long.parseLong((String) object);
          }
        }
        // Delegate all other interpretation to SpectatorHistogram
        return SpectatorHistogram.deserialize(object);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    final SpectatorHistogramIndexed column = SpectatorHistogramIndexed.read(
        buffer,
        STRATEGY
    );
    builder.setComplexColumnSupplier(new SpectatorHistogramColumnPartSupplier(this.typeName, column));
  }

  @Override
  public ObjectStrategy<SpectatorHistogram> getObjectStrategy()
  {
    return STRATEGY;
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return SpectatorHistogramSerializer.create(
        segmentWriteOutMedium,
        column,
        this.getObjectStrategy()
    );
  }

}
