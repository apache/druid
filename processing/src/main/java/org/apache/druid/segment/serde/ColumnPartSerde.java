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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "complex", value = ComplexColumnPartSerde.class),
    @JsonSubTypes.Type(name = "float", value = FloatNumericColumnPartSerde.class),
    @JsonSubTypes.Type(name = "long", value = LongNumericColumnPartSerde.class),
    @JsonSubTypes.Type(name = "double", value = DoubleNumericColumnPartSerde.class),
    @JsonSubTypes.Type(name = "stringDictionary", value = DictionaryEncodedColumnPartSerde.class),
    @JsonSubTypes.Type(name = "floatV2", value = FloatNumericColumnPartSerdeV2.class),
    @JsonSubTypes.Type(name = "longV2", value = LongNumericColumnPartSerdeV2.class),
    @JsonSubTypes.Type(name = "doubleV2", value = DoubleNumericColumnPartSerdeV2.class),
})
public interface ColumnPartSerde
{
  @Nullable
  Serializer getSerializer();

  Deserializer getDeserializer();

  interface Deserializer
  {
    void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig);
  }
}
