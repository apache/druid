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
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.DoubleColumnSerializer;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedDoublesIndexedSupplier;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class DoubleGenericColumnPartSerde implements ColumnPartSerde
{
  private final ByteOrder byteOrder;
  private  Serializer serialize;

  @JsonCreator
  public static DoubleGenericColumnPartSerde getDoubleGenericColumnPartSerde(
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new DoubleGenericColumnPartSerde(byteOrder, null);
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }


  public DoubleGenericColumnPartSerde(ByteOrder byteOrder, Serializer serialize)
  {
    this.byteOrder = byteOrder;
    this.serialize = serialize;
  }

  @Override
  public Serializer getSerializer()
  {
    return serialize;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return (buffer, builder, columnConfig) -> {
      final CompressedDoublesIndexedSupplier column = CompressedDoublesIndexedSupplier.fromByteBuffer(
            buffer,
            byteOrder,
            builder.getFileMapper()
        );
        builder.setType(ValueType.DOUBLE)
               .setHasMultipleValues(false)
               .setGenericColumn(new DoubleGenericColumnSupplier(column));

    };
  }

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static class SerializerBuilder
  {
    private ByteOrder byteOrder = null;
    private DoubleColumnSerializer delegate = null;

    public
    SerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public SerializerBuilder withDelegate(final DoubleColumnSerializer delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public DoubleGenericColumnPartSerde build()
    {
      return new DoubleGenericColumnPartSerde(
          byteOrder,
          new Serializer()
          {
            @Override
            public long numBytes()
            {
              return delegate.getSerializedSize();
            }

            @Override
            public void write(WritableByteChannel channel, FileSmoosher fileSmoosher) throws IOException
            {
              delegate.writeToChannel(channel, fileSmoosher);
            }
          }
      );
    }
  }
}
