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

package org.apache.druid.query.aggregation;

import com.google.common.primitives.Longs;
import org.apache.druid.collections.SerializablePair;
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

/**
 * The class serializes a Long-X pair object
 * <p>
 * Note: T must be a concrete class which inherits from SerializablePair, such as {@link SerializablePairLongDouble}
 */
public abstract class AbstractSerializablePairSerde<T extends SerializablePair<Long, ?>> extends ComplexMetricSerde
{
  private final Class<T> pairClassObject;

  public AbstractSerializablePairSerde(Class<T> pairClassObject)
  {
    this.pairClassObject = pairClassObject;
  }

  @Override
  public ComplexMetricExtractor<T> getExtractor()
  {
    return new ComplexMetricExtractor<T>()
    {
      @Override
      public Class<T> extractedClass()
      {
        return pairClassObject;
      }

      @Override
      public T extractValue(InputRow inputRow, String metricName)
      {
        return (T) inputRow.getRaw(metricName);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy(), columnBuilder.getFileMapper());
    columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy<T> getObjectStrategy()
  {
    return new ObjectStrategy<T>()
    {
      @Override
      public int compare(@Nullable T o1, @Nullable T o2)
      {
        return Longs.compare(o1.lhs, o2.lhs);
      }

      @Override
      public Class<T> getClazz()
      {
        return pairClassObject;
      }

      @Override
      public T fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        return toPairObject(buffer, numBytes);
      }

      @Override
      public byte[] toBytes(T val)
      {
        return pairToBytes(val);
      }
    };
  }

  @Override
  public GenericColumnSerializer<T> getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }

  protected abstract T toPairObject(ByteBuffer buffer, int numBytes);

  protected abstract byte[] pairToBytes(T val);
}
