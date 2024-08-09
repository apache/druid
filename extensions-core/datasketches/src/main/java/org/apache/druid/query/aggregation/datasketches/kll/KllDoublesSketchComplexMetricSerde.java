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

package org.apache.druid.query.aggregation.datasketches.kll;

import com.google.common.primitives.Doubles;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

public class KllDoublesSketchComplexMetricSerde extends ComplexMetricSerde
{

  private static final KllDoublesSketchObjectStrategy STRATEGY = new KllDoublesSketchObjectStrategy();

  @Override
  public String getTypeName()
  {
    return KllSketchModule.DOUBLES_SKETCH;
  }

  @Override
  public ObjectStrategy<KllDoublesSketch> getObjectStrategy()
  {
    return STRATEGY;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      private static final int MIN_K = 8; // package one input value into the smallest sketch

      @Override
      public Class<?> extractedClass()
      {
        return KllDoublesSketch.class;
      }

      @Override
      public Object extractValue(final InputRow inputRow, final String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object instanceof String) { // everything is a string during ingestion
          final String objectString = (String) object;
          // Autodetection of the input format: empty string, number, or base64 encoded sketch
          // A serialized KllDoublesSketch, as currently implemented, always has 0 in the first 5 bits.
          // This is not a digit in base64
          final Double doubleValue;
          if (objectString.isEmpty()) {
            return KllDoublesSketchOperations.EMPTY_SKETCH;
          } else if ((doubleValue = Doubles.tryParse(objectString)) != null) {
            final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(MIN_K);
            sketch.update(doubleValue);
            return sketch;
          }
        } else if (object instanceof Number) { // this is for reindexing
          final KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(MIN_K);
          sketch.update(((Number) object).doubleValue());
          return sketch;
        }

        if (object == null || object instanceof KllDoublesSketch || object instanceof Memory) {
          return object;
        }
        return KllDoublesSketchOperations.deserializeSafe(object);
      }
    };
  }
}
