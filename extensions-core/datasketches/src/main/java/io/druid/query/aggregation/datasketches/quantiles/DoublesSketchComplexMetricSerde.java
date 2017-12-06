/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.quantiles;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.IAE;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;

public class DoublesSketchComplexMetricSerde extends ComplexMetricSerde
{

  private static final DoublesSketchObjectStrategy strategy = new DoublesSketchObjectStrategy();

  @Override
  public String getTypeName()
  {
    return DoublesSketchModule.DOUBLES_SKETCH;
  }

  @Override
  public ObjectStrategy<DoublesSketch> getObjectStrategy()
  {
    return strategy;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      private static final int MIN_K = 2; // package one input value into the smallest sketch

      @Override
      public Class<?> extractedClass()
      {
        return DoublesSketch.class;
      }

      @Override
      public Object extractValue(final InputRow inputRow, final String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object instanceof String) { // everything is a string during ingestion
          String objectString = (String) object;
          // Autodetection of the input format: a number or base64 encoded sketch
          // A serialized DoublesSketch, as currently implemented, always has 0 in the first 6 bits.
          // This corresponds to "A" in base64, so it is not a digit
          if (Character.isDigit((objectString).charAt(0))) {
            try {
              Double doubleValue = Double.parseDouble(objectString);
              UpdateDoublesSketch sketch = DoublesSketch.builder().setK(MIN_K).build();
              sketch.update(doubleValue);
              return sketch;
            }
            catch (NumberFormatException e) {
              throw new IAE("Expected a string with a number, received value " + objectString);
            }
          }
        } else if (object instanceof Number) { // this is for reindexing
          UpdateDoublesSketch sketch = DoublesSketch.builder().setK(MIN_K).build();
          sketch.update(((Number) object).doubleValue());
          return sketch;
        }

        if (object == null || object instanceof DoublesSketch || object instanceof Memory) {
          return object;
        }
        return DoublesSketchOperations.deserialize(object);
      }
    };
  }

  @Override
  public void deserializeColumn(final ByteBuffer buffer, final ColumnBuilder builder)
  {
    final GenericIndexed<DoublesSketch> column = GenericIndexed.read(buffer, strategy, builder.getFileMapper());
    builder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  // support large columns
  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }
}
