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
import io.druid.segment.LongColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.CompressedLongsIndexedSupplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 */
public class LongGenericColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static LongGenericColumnPartSerde createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    return new LongGenericColumnPartSerde(byteOrder, null);
  }

  private final ByteOrder byteOrder;
  private Serializer serializer;

  private LongGenericColumnPartSerde(ByteOrder byteOrder, Serializer serializer)
  {
    this.byteOrder = byteOrder;
    this.serializer = serializer;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static class SerializerBuilder
  {
    private ByteOrder byteOrder = null;
    private LongColumnSerializer delegate = null;

    public SerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public SerializerBuilder withDelegate(final LongColumnSerializer delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public LongGenericColumnPartSerde build()
    {
      return new LongGenericColumnPartSerde(
          byteOrder, new Serializer()
      {
        @Override
        public long numBytes()
        {
          return delegate.getSerializedSize();
        }

        @Override
        public void write(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
        {
          delegate.writeToChannel(channel, smoosher);
        }
      }
      );
    }
  }

  public static LegacySerializerBuilder legacySerializerBuilder()
  {
    return new LegacySerializerBuilder();
  }

  public static class LegacySerializerBuilder
  {
    private ByteOrder byteOrder = null;
    private CompressedLongsIndexedSupplier delegate = null;

    public LegacySerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public LegacySerializerBuilder withDelegate(final CompressedLongsIndexedSupplier delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public LongGenericColumnPartSerde build()
    {
      return new LongGenericColumnPartSerde(
          byteOrder, new Serializer()
      {
        @Override
        public long numBytes()
        {
          return delegate.getSerializedSize();
        }

        @Override
        public void write(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
        {
          delegate.writeToChannel(channel);
        }
      }
      );
    }
  }

  @Override
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
      {
        final CompressedLongsIndexedSupplier column = CompressedLongsIndexedSupplier.fromByteBuffer(
            buffer,
            byteOrder,
            builder.getFileMapper()
        );
        builder.setType(ValueType.LONG)
               .setHasMultipleValues(false)
               .setGenericColumn(new LongGenericColumnSupplier(column));
      }
    };
  }
}
