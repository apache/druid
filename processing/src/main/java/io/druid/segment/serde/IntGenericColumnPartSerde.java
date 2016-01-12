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
import io.druid.segment.data.CompressedIntsIndexedSupplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
*/
public class IntGenericColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static IntGenericColumnPartSerde createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new IntGenericColumnPartSerde(null, byteOrder);
  }

  private final CompressedIntsIndexedSupplier compressedInts;
  private final ByteOrder byteOrder;

  public IntGenericColumnPartSerde(CompressedIntsIndexedSupplier compressedInts, ByteOrder byteOrder)
  {
    this.compressedInts = compressedInts;
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
    return compressedInts.getSerializedSize();
  }

  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    compressedInts.writeToChannel(channel);
  }

  @Override
  public ColumnPartSerde read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
  {
    final CompressedIntsIndexedSupplier column = CompressedIntsIndexedSupplier.fromByteBuffer(buffer, byteOrder);

    builder.setType(ValueType.LONG)
           .setHasMultipleValues(false)
           .setGenericColumn(new IntGenericColumnSupplier(column));

    return new IntGenericColumnPartSerde(column, byteOrder);
  }
}
