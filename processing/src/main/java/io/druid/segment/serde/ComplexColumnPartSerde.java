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
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
public class ComplexColumnPartSerde implements ColumnPartSerde
{
  private final String typeName;
  private final ComplexMetricSerde serde;
  private final Serializer serializer;
  private ComplexColumnPartSerde(String typeName, Serializer serializer)
  {
    this.typeName = typeName;
    this.serde = ComplexMetrics.getSerdeForType(typeName);
    this.serializer = serializer;
  }

  @JsonCreator
  public static ComplexColumnPartSerde createDeserializer(
      @JsonProperty("typeName") String complexType
  )
  {
    return new ComplexColumnPartSerde(complexType, null);
  }

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  @JsonProperty
  public String getTypeName()
  {
    return typeName;
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
        if (serde != null) {
          serde.deserializeColumn(buffer, builder);
        }
      }
    };
  }

  public static class SerializerBuilder
  {
    private String typeName = null;
    private GenericColumnSerializer delegate = null;

    public SerializerBuilder withTypeName(final String typeName)
    {
      this.typeName = typeName;
      return this;
    }

    public SerializerBuilder withDelegate(final GenericColumnSerializer delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public ComplexColumnPartSerde build()
    {
      return new ComplexColumnPartSerde(
          typeName, new Serializer()
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
}
