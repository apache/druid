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

package io.druid.query.aggregation.datasketches.tuple.doublearray;

import java.nio.ByteBuffer;

import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;

import io.druid.data.input.InputRow;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

/**
 * 
 * @author sunxin@rongcapital.cn
 *
 */
@SuppressWarnings("unchecked")
public class SketchMergeComplexMetricSerde extends ComplexMetricSerde
{
  private SketchObjectStrategy strategy = new SketchObjectStrategy();

  @Override
  public String getTypeName()
  {
    return SketchModule.TUPLE_DOUBLE_SKETCH;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return ArrayOfDoublesSketch.class;
      }

      @Override
      public Object extractValue(InputRow inputRow, String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object == null || object instanceof ArrayOfDoublesSketch || object instanceof Memory) {
          return object;
        }
        return SketchOperations.deserialize(object);

      }
    };
  }


  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    GenericIndexed<ArrayOfDoublesSketch> ge = GenericIndexed.read(buffer, strategy);
    builder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), ge));
  }

  @Override
  public ObjectStrategy<ArrayOfDoublesSketch> getObjectStrategy()
  {
    return strategy;
  }

}
