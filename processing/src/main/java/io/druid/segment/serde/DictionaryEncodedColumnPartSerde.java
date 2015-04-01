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
import com.google.common.base.Preconditions;
import com.metamx.common.ISE;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;


public class DictionaryEncodedColumnPartSerde implements ColumnPartSerde
{
  protected static enum TYPE {
    UNCOMPRESSED_SINGLE_VALUE,             // 0x0
    UNCOMPRESSED_MULTI_VALUE,              // 0x1
    COMPRESSED;  // 0x2

    public static TYPE fromByte(byte b) {
      final TYPE[] values = TYPE.values();
      Preconditions.checkArgument(b < values.length, "Unknown dictionary column type 0x%X", b);
      return values[b];
    }

    public byte asByte() {
      return (byte)this.ordinal();
    }
  }

  protected final BitmapSerdeFactory bitmapSerdeFactory;
  protected final ByteOrder byteOrder;

  @JsonCreator
  public DictionaryEncodedColumnPartSerde(
      @JsonProperty("bitmapSerdeFactory") BitmapSerdeFactory bitmapSerdeFactory,
      @JsonProperty("byteOrder") ByteOrder byteOrder
  )
  {
    this.bitmapSerdeFactory = bitmapSerdeFactory == null
                              ? new BitmapSerde.LegacyBitmapSerdeFactory()
                              : bitmapSerdeFactory;
    this.byteOrder = byteOrder;
  }

  @JsonProperty
  public BitmapSerdeFactory getBitmapSerdeFactory()
  {
    return bitmapSerdeFactory;
  }


  @JsonProperty
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }


  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    throw new UnsupportedOperationException("method must be called on an actual implementation");
  }

  @Override
  public ColumnPartSerde read(
      ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig
  )
  {
    // read without modifying position
    final byte version = buffer.get(buffer.position());
    final TYPE type = TYPE.fromByte(version);
    switch (type) {
      case UNCOMPRESSED_SINGLE_VALUE:
      case UNCOMPRESSED_MULTI_VALUE:
        return new LegacyDictionaryEncodedColumnPartSerde(bitmapSerdeFactory, byteOrder).read(buffer, builder, columnConfig);

      case COMPRESSED:
        return new CompressedDictionaryEncodedColumnPartSerde(bitmapSerdeFactory, byteOrder).read(buffer, builder, columnConfig);

      default:
        throw new ISE("Unknown column version[%d]", version);
    }
  }

  @Override
  public long numBytes()
  {
    throw new UnsupportedOperationException("method must be called on an actual implementation");
  }
}
