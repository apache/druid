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

package org.apache.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ColumnarDoubles;
import org.apache.druid.segment.data.CompressedColumnarDoublesSuppliers;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 */
public class DoubleNumericColumnPartSerdeV2 implements ColumnPartSerde
{
  @JsonCreator
  public static DoubleNumericColumnPartSerdeV2 getDoubleGenericColumnPartSerde(
      @JsonProperty("byteOrder") ByteOrder byteOrder,
      @JsonProperty("bitmapSerdeFactory") @Nullable BitmapSerdeFactory bitmapSerdeFactory
  )
  {
    return new DoubleNumericColumnPartSerdeV2(
        byteOrder,
        bitmapSerdeFactory != null ? bitmapSerdeFactory : new BitmapSerde.LegacyBitmapSerdeFactory(),
        null
    );
  }

  private final ByteOrder byteOrder;
  @Nullable
  private final Serializer serializer;
  private final BitmapSerdeFactory bitmapSerdeFactory;

  public DoubleNumericColumnPartSerdeV2(
      ByteOrder byteOrder,
      BitmapSerdeFactory bitmapSerdeFactory,
      @Nullable Serializer serializer
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
    @Nullable
    private ByteOrder byteOrder = null;
    @Nullable
    private Serializer delegate = null;
    @Nullable
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

    public DoubleNumericColumnPartSerdeV2 build()
    {
      Serializer serializer = new Serializer()
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
      };
      return new DoubleNumericColumnPartSerdeV2(byteOrder, bitmapSerdeFactory, serializer);
    }
  }

  @Nullable
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
      final Supplier<ColumnarDoubles> column = CompressedColumnarDoublesSuppliers.fromByteBuffer(
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
      builder.setType(ValueType.DOUBLE)
             .setHasMultipleValues(false)
             .setNumericColumnSupplier(new DoubleNumericColumnSupplier(column, bitmap));
    };
  }
}
