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
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;

public class HistogramSerde extends ComplexMetricSerde
{

  @Override
  public String getTypeName()
  {
    return Histogram.TYPE.getComplexTypeName();
  }

  @Override
  public ComplexMetricExtractor<Histogram> getExtractor()
  {
    return new ComplexMetricExtractor<Histogram>()
    {
      @Override
      public Class<? extends Histogram> extractedClass()
      {
        return Histogram.class;
      }

      @Override
      public Histogram extractValue(InputRow inputRow, String metricName)
      {
        Object rawValue = inputRow.getRaw(metricName);
        if (rawValue instanceof Histogram) {
          return (Histogram) rawValue;
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
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed column = GenericIndexed.read(byteBuffer, getObjectStrategy(), columnBuilder.getFileMapper());
    columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy<Histogram> getObjectStrategy()
  {
    return new ObjectStrategy<Histogram>()
    {

      @Override
      public int compare(Histogram o1, Histogram o2)
      {
        return 0;
      }

      @Override
      public Class<? extends Histogram> getClazz()
      {
        return Histogram.class;
      }

      @Override
      public Histogram fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        return Histogram.fromBytes(buffer);
      }

      @Override
      public byte[] toBytes(Histogram val)
      {
        return val.toBytes();
      }
    };
  }

  protected Histogram fromBytes(byte[] rawValue)
  {
    ByteBuffer bb = ByteBuffer.wrap(rawValue);
    return getObjectStrategy().fromByteBuffer(bb, bb.limit());
  }

}
