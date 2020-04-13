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

package org.apache.druid.query.aggregation.datasketches.theta;

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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SketchMergeComplexMetricSerde extends ComplexMetricSerde
{
  private SketchHolderObjectStrategy strategy = new SketchHolderObjectStrategy();

  @Override
  public String getTypeName()
  {
    return SketchModule.THETA_SKETCH;
  }

  @Override
  public ComplexMetricExtractor<?> getExtractor()
  {
    return new ComplexMetricExtractor<SketchHolder>()
    {
      @Override
      public Class<SketchHolder> extractedClass()
      {
        return SketchHolder.class;
      }

      @Override
      @Nullable
      public SketchHolder extractValue(InputRow inputRow, String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        return object == null ? null : SketchHolder.deserialize(object);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    GenericIndexed<SketchHolder> ge = GenericIndexed.read(buffer, strategy, builder.getFileMapper());
    builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), ge));
  }

  @Override
  public ObjectStrategy<SketchHolder> getObjectStrategy()
  {
    return strategy;
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }

}
