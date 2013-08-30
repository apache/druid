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
import io.druid.segment.data.GenericIndexed;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
*/
public class ComplexColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static ComplexColumnPartSerde createDeserializer(
      @JsonProperty("typeName") String complexType
  )
  {
    return new ComplexColumnPartSerde(null, complexType);
  }

  private final GenericIndexed column;
  private final String typeName;

  private final ComplexMetricSerde serde;

  public ComplexColumnPartSerde(GenericIndexed column, String typeName)
  {
    this.column = column;
    this.typeName = typeName;
    serde = ComplexMetrics.getSerdeForType(typeName);
  }

  @JsonProperty
  public String getTypeName()
  {
    return typeName;
  }

  @Override
  public long numBytes()
  {
    return column.getSerializedSize();
  }

  @Override
  public void write(WritableByteChannel channel) throws IOException
  {
    column.writeToChannel(channel);
  }

  @Override
  public ColumnPartSerde read(ByteBuffer buffer, ColumnBuilder builder)
  {
    return serde == null ? this : serde.deserializeColumn(buffer, builder);
  }
}
