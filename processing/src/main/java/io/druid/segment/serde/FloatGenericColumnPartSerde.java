/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedFloatsIndexedSupplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
*/
public class FloatGenericColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static FloatGenericColumnPartSerde createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new FloatGenericColumnPartSerde(null, byteOrder);
  }

  private final CompressedFloatsIndexedSupplier compressedFloats;
  private final ByteOrder byteOrder;

  public FloatGenericColumnPartSerde(CompressedFloatsIndexedSupplier compressedFloats, ByteOrder byteOrder)
  {
    this.compressedFloats = compressedFloats;
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
    return compressedFloats.getSerializedSize();
  }

  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    compressedFloats.writeToChannel(channel);
  }

  @Override
  public ColumnPartSerde read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
  {
    final CompressedFloatsIndexedSupplier column = CompressedFloatsIndexedSupplier.fromByteBuffer(buffer, byteOrder);

    builder.setType(ValueType.FLOAT)
           .setHasMultipleValues(false)
           .setGenericColumn(new FloatGenericColumnSupplier(column, byteOrder));

    return new FloatGenericColumnPartSerde(column, byteOrder);
  }
}
