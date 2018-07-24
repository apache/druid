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

package io.druid.query.aggregation.datasketches.tuple;

import java.nio.ByteBuffer;

import io.druid.data.input.InputRow;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;

public class ArrayOfDoublesSketchMergeComplexMetricSerde extends ComplexMetricSerde
{

  @Override
  public String getTypeName()
  {
    return ArrayOfDoublesSketchModule.ARRAY_OF_DOUBLES_SKETCH;
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
      public Object extractValue(final InputRow inputRow, final String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object == null || object instanceof ArrayOfDoublesSketch) {
          return object;
        }
        return ArrayOfDoublesSketchOperations.deserialize(object);
      }
    };
  }

  @Override
  public void deserializeColumn(final ByteBuffer buffer, final ColumnBuilder builder)
  {
    final GenericIndexed<ArrayOfDoublesSketch> ge = GenericIndexed.read(buffer, ArrayOfDoublesSketchObjectStrategy.STRATEGY);
    builder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), ge));
  }

  @Override
  public ObjectStrategy<ArrayOfDoublesSketch> getObjectStrategy()
  {
    return ArrayOfDoublesSketchObjectStrategy.STRATEGY;
  }

  // support large columns
  @Override
  public GenericColumnSerializer getSerializer(final SegmentWriteOutMedium segmentWriteOutMedium, final String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }

}
