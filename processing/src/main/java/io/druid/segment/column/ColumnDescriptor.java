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

package io.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 */
public class ColumnDescriptor
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final ValueType valueType;
  private final boolean hasMultipleValues;
  private final List<ColumnPartSerde> parts;

  @JsonCreator
  public ColumnDescriptor(
      @JsonProperty("valueType") ValueType valueType,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("parts") List<ColumnPartSerde> parts
  )
  {
    this.valueType = valueType;
    this.hasMultipleValues = hasMultipleValues;
    this.parts = parts;
  }

  @JsonProperty
  public ValueType getValueType()
  {
    return valueType;
  }

  @JsonProperty
  public boolean isHasMultipleValues()
  {
    return hasMultipleValues;
  }

  @JsonProperty
  public List<ColumnPartSerde> getParts()
  {
    return parts;
  }

  public long numBytes()
  {
    long retVal = 0;

    for (ColumnPartSerde part : parts) {
      retVal += part.numBytes();
    }

    return retVal;
  }

  public void write(WritableByteChannel channel) throws IOException
  {
    for (ColumnPartSerde part : parts) {
      part.write(channel);
    }
  }

  public Column read(ByteBuffer buffer)
  {
    final ColumnBuilder builder = new ColumnBuilder()
        .setType(valueType)
        .setHasMultipleValues(hasMultipleValues);

    for (ColumnPartSerde part : parts) {
      part.read(buffer, builder);
    }

    return builder.build();
  }

  public static class Builder
  {
    private ValueType valueType = null;
    private Boolean hasMultipleValues = null;

    private final List<ColumnPartSerde> parts = Lists.newArrayList();

    public Builder setValueType(ValueType valueType)
    {
      if (this.valueType != null && this.valueType != valueType) {
        throw new IAE("valueType[%s] is already set, cannot change to[%s]", this.valueType, valueType);
      }
      this.valueType = valueType;
      return this;
    }

    public Builder setHasMultipleValues(boolean hasMultipleValues)
    {
      if (this.hasMultipleValues != null && this.hasMultipleValues != hasMultipleValues) {
        throw new IAE(
            "hasMultipleValues[%s] is already set, cannot change to[%s]", this.hasMultipleValues, hasMultipleValues
        );
      }
      this.hasMultipleValues = hasMultipleValues;
      return this;
    }

    public Builder addSerde(ColumnPartSerde serde)
    {
      parts.add(serde);
      return this;
    }

    public ColumnDescriptor build()
    {
      Preconditions.checkNotNull(valueType, "must specify a valueType");
      return new ColumnDescriptor(valueType, hasMultipleValues == null ? false : hasMultipleValues, parts);
    }
  }
}
