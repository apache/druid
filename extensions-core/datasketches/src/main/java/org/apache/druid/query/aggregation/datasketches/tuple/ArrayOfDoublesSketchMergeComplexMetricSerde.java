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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.yahoo.sketches.tuple.ArrayOfDoublesSketch;
import org.apache.druid.data.input.InputRow;
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
    builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), ge));
  }

  @Override
  public ObjectStrategy<ArrayOfDoublesSketch> getObjectStrategy()
  {
    return ArrayOfDoublesSketchObjectStrategy.STRATEGY;
  }

  @Override
  public GenericColumnSerializer getSerializer(final SegmentWriteOutMedium segmentWriteOutMedium, final String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }

}
