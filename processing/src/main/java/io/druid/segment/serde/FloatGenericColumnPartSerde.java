/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.segment.column.ColumnBuilder;
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
  public ColumnPartSerde read(ByteBuffer buffer, ColumnBuilder builder)
  {
    final CompressedFloatsIndexedSupplier column = CompressedFloatsIndexedSupplier.fromByteBuffer(buffer, byteOrder);

    builder.setType(ValueType.FLOAT)
           .setHasMultipleValues(false)
           .setGenericColumn(new FloatGenericColumnSupplier(column, byteOrder));

    return new FloatGenericColumnPartSerde(column, byteOrder);
  }
}
