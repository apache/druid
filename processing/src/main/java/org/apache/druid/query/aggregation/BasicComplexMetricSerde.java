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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;

public abstract class BasicComplexMetricSerde<T> extends ComplexMetricSerde
{
  private final ColumnType type;
  private final ObjectStrategy<T> objectStrategy;

  public BasicComplexMetricSerde(ColumnType type, ObjectStrategy<T> objectStrategy)
  {
    this.type = type;
    this.objectStrategy = objectStrategy;
  }

  @Override
  public final String getTypeName()
  {
    return type.getComplexTypeName();
  }

  @Override
  public final ComplexMetricExtractor<T> getExtractor()
  {
    return new ObjectStrategyBasedComplexMetricExtractor<T>(objectStrategy);
  }

  @Override
  public final void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy(), columnBuilder.getFileMapper());
    columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy<T> getObjectStrategy()
  {
    return objectStrategy;
  }

  static class ObjectStrategyBasedComplexMetricExtractor<T> implements ComplexMetricExtractor<T>
  {
    private final ObjectStrategy<T> objectStrategy;
    private final Class<T> clazz;

    public ObjectStrategyBasedComplexMetricExtractor(ObjectStrategy<T> objectStrategy)
    {
      this.objectStrategy = objectStrategy;
      this.clazz = (Class<T>) objectStrategy.getClazz();
    }

    @Override
    public Class<T> extractedClass()
    {
      return clazz;
    }

    @Override
    public T extractValue(InputRow inputRow, String metricName)
    {
      Object rawValue = inputRow.getRaw(metricName);
      if (clazz.isInstance(rawValue)) {
        return (T) rawValue;
      }
      if (rawValue instanceof byte[]) {
        return fromBytes((byte[]) rawValue);
      }
      throw new ISE(
          "Object is of type[%s] that can not deserialized to type[%s].",
          rawValue.getClass().getName(),
          Histogram.class.getName()
      );
    }

    private T fromBytes(byte[] rawValue)
    {
      ByteBuffer bb = ByteBuffer.wrap(rawValue);
      return objectStrategy.fromByteBuffer(bb, bb.limit());
    }
  }
}
