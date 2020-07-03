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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.GenericColumnSerializer;

import javax.annotation.Nullable;

/**
 */
public class ComplexColumnPartSerde implements ColumnPartSerde
{
  private final String typeName;
  @Nullable
  private final ComplexMetricSerde serde;
  private final Serializer serializer;
  private static final Logger log = new Logger(ComplexColumnPartSerde.class);

  private ComplexColumnPartSerde(String typeName, Serializer serializer)
  {
    this.typeName = typeName;
    this.serde = ComplexMetrics.getSerdeForType(typeName);
    if (this.serde == null) {
      // Not choosing to fail here since this gets handled as
      // an UnknownTypeComplexColumn. See SimpleColumnHolder#getColumn.
      log.warn("Unknown complex column of type %s detected", typeName);
    }
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
    return (buffer, builder, columnConfig) -> {
      if (serde != null) {
        serde.deserializeColumn(buffer, builder);
      }
    };
  }

  public static class SerializerBuilder
  {
    @Nullable
    private String typeName = null;
    @Nullable
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
      return new ComplexColumnPartSerde(typeName, delegate);
    }
  }
}
