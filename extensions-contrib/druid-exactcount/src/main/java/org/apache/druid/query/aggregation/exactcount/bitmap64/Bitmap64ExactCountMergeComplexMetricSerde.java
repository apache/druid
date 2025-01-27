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

package org.apache.druid.query.aggregation.exactcount.bitmap64;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
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

public class Bitmap64ExactCountMergeComplexMetricSerde extends ComplexMetricSerde
{

  static RoaringBitmap64Counter deserializeRoaringBitmap64Counter(final Object object)
  {
    if (object instanceof String) {
      return RoaringBitmap64Counter.fromBytes(StringUtils.decodeBase64(StringUtils.toUtf8((String) object)));
    } else if (object instanceof byte[]) {
      return RoaringBitmap64Counter.fromBytes((byte[]) object);
    } else if (object instanceof RoaringBitmap64Counter) {
      return (RoaringBitmap64Counter) object;
    }
    throw new IAE("Object is not of a type that can be deserialized to an RoaringBitmap64Counter:" + object.getClass()
                                                                                                           .getName());
  }

  @Override
  public String getTypeName()
  {
    return Bitmap64ExactCountModule.TYPE_NAME; // must be common type name
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return Bitmap64ExactCountObjectStrategy.STRATEGY;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return RoaringBitmap64Counter.class;
      }

      @Override
      public RoaringBitmap64Counter extractValue(final InputRow inputRow, final String metricName)
      {
        final Object object = inputRow.getRaw(metricName);
        if (object == null) {
          return null;
        }
        return deserializeRoaringBitmap64Counter(object);
      }
    };
  }

  @Override
  public void deserializeColumn(final ByteBuffer buf, final ColumnBuilder columnBuilder)
  {
    columnBuilder.setComplexColumnSupplier(
        new ComplexColumnPartSupplier(
            getTypeName(),
            GenericIndexed.read(buf, Bitmap64ExactCountObjectStrategy.STRATEGY, columnBuilder.getFileMapper())
        )
    );
  }

  // support large columns
  @Override
  public GenericColumnSerializer getSerializer(final SegmentWriteOutMedium segmentWriteOutMedium, final String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }

}
