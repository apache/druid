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
  public ColumnPartSerde read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
  {
    return serde == null ? this : serde.deserializeColumn(buffer, builder);
  }
}
