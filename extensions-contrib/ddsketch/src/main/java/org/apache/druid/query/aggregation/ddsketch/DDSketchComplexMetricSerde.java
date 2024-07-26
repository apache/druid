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

package org.apache.druid.query.aggregation.ddsketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;


public class DDSketchComplexMetricSerde extends ComplexMetricSerde
{
  private static final DDSketchObjectStrategy STRATEGY = new DDSketchObjectStrategy();

  @Override
  public String getTypeName()
  {
    return DDSketchAggregatorFactory.TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return DDSketch.class;
      }

      @Override
      public Object extractValue(final InputRow inputRow, final String metricName)
      {
        final Object obj = inputRow.getRaw(metricName);
        if (obj == null || obj instanceof Number || obj instanceof DDSketch) {
          return obj;
        }
        if (obj instanceof String) {
          String objString = (String) obj;
          if (objString.isEmpty()) {
            return null;
          }

          try {
            Double doubleValue = Double.parseDouble(objString);
            return doubleValue;
          }
          catch (NumberFormatException e) {
            throw new IAE("Expected string with a number, received value: " + objString);
          }

        }

        return DDSketchUtils.deserialize(obj);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    final GenericIndexed<DDSketch> column = GenericIndexed.read(
        buffer,
        STRATEGY,
        builder.getFileMapper()
    );
    builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy<DDSketch> getObjectStrategy()
  {
    return STRATEGY;
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(
        segmentWriteOutMedium,
        column,
        this.getObjectStrategy()
    );
  }

}
