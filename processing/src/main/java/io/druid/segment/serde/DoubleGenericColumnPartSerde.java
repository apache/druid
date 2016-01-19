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

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedDoublesIndexedSupplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
*/
public class DoubleGenericColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static DoubleGenericColumnPartSerde createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new DoubleGenericColumnPartSerde(null, byteOrder);
  }

  private final CompressedDoublesIndexedSupplier compressedDoubles;
  private final ByteOrder byteOrder;

  public DoubleGenericColumnPartSerde(CompressedDoublesIndexedSupplier compressedDoubles, ByteOrder byteOrder)
  {
    this.compressedDoubles = compressedDoubles;
    this.byteOrder = byteOrder;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  @Override
  public long numBytes()
  {
    return compressedDoubles.getSerializedSize();
  }

  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    compressedDoubles.writeToChannel(channel);
  }

  @Override
  public ColumnPartSerde read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
  {
    final CompressedDoublesIndexedSupplier column = CompressedDoublesIndexedSupplier.fromByteBuffer(buffer, byteOrder);

    builder.setType(ValueType.FLOAT)
           .setHasMultipleValues(false)
           .setGenericColumn(new DoubleGenericColumnSupplier(column, byteOrder));

    return new DoubleGenericColumnPartSerde(column, byteOrder);
  }
}
