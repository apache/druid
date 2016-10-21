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

package io.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.java.util.common.IAE;
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
      retVal += part.getSerializer().numBytes();
    }

    return retVal;
  }

  public void write(WritableByteChannel channel) throws IOException
  {
    for (ColumnPartSerde part : parts) {
      part.getSerializer().write(channel);
    }
  }

  public Column read(ByteBuffer buffer, ColumnConfig columnConfig)
  {
    final ColumnBuilder builder = new ColumnBuilder()
        .setType(valueType)
        .setHasMultipleValues(hasMultipleValues);

    for (ColumnPartSerde part : parts) {
      part.getDeserializer().read(buffer, builder, columnConfig);
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
