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
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.CompressedColumnarFloatsSupplier;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 */
public class FloatGenericColumnPartSerdeV2 implements ColumnPartSerde
{
  @JsonCreator
  public static FloatGenericColumnPartSerdeV2 createDeserializer(
      @JsonProperty("byteOrder") ByteOrder byteOrder,
      @Nullable @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    return new FloatGenericColumnPartSerdeV2(
        byteOrder,
        bitmapSerdeFactory != null ? bitmapSerdeFactory : new BitmapSerde.LegacyBitmapSerdeFactory(),
        null
    );
  }

  private final ByteOrder byteOrder;
  private Serializer serializer;
  private final BitmapSerdeFactory bitmapSerdeFactory;

  private FloatGenericColumnPartSerdeV2(
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      Serializer serializer
  )
  {
    this.byteOrder = byteOrder;
    this.bitmapSerdeFactory = bitmapSerdeFactory;
    this.serializer = serializer;
  }

  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static class SerializerBuilder
  {
    private ByteOrder byteOrder = null;
    private Serializer delegate = null;
    private BitmapSerdeFactory bitmapSerdeFactory = null;

    public SerializerBuilder withByteOrder(final ByteOrder byteOrder)
    {
      this.byteOrder = byteOrder;
      return this;
    }

    public SerializerBuilder withDelegate(final Serializer delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public SerializerBuilder withBitmapSerdeFactory(BitmapSerdeFactory bitmapSerdeFactory)
    {
      this.bitmapSerdeFactory = bitmapSerdeFactory;
      return this;
    }

    public FloatGenericColumnPartSerdeV2 build()
    {
      return new FloatGenericColumnPartSerdeV2(
          byteOrder, bitmapSerdeFactory,
          new Serializer()
          {
            @Override
            public long getSerializedSize() throws IOException
            {
              return delegate.getSerializedSize();
            }

            @Override
            public void writeTo(WritableByteChannel channel, FileSmoosher fileSmoosher) throws IOException
            {
              delegate.writeTo(channel, fileSmoosher);
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
    return (buffer, builder, columnConfig) -> {
      int offset = buffer.getInt();
      int initialPos = buffer.position();
      final CompressedColumnarFloatsSupplier column = CompressedColumnarFloatsSupplier.fromByteBuffer(
          buffer,
          byteOrder
      );
      buffer.position(initialPos + offset);
      final ImmutableBitmap bitmap;
      if (buffer.hasRemaining()) {
        bitmap = bitmapSerdeFactory.getObjectStrategy().fromByteBufferWithSize(buffer);
      } else {
        bitmap = bitmapSerdeFactory.getBitmapFactory().makeEmptyImmutableBitmap();
      }
      builder.setType(ValueType.FLOAT)
             .setHasMultipleValues(false)
             .setGenericColumn(new FloatGenericColumnSupplier(column, bitmap));
    };
  }
}
