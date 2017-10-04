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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "complex", value = ComplexColumnPartSerde.class),
    @JsonSubTypes.Type(name = "float", value = FloatGenericColumnPartSerde.class),
    @JsonSubTypes.Type(name = "long", value = LongGenericColumnPartSerde.class),
    @JsonSubTypes.Type(name = "double", value = DoubleGenericColumnPartSerde.class),
    @JsonSubTypes.Type(name = "stringDictionary", value = DictionaryEncodedColumnPartSerde.class)
})
public interface ColumnPartSerde
{
  public Serializer getSerializer();

  public Deserializer getDeserializer();

  public interface Serializer
  {
    public long numBytes();

    public void write(WritableByteChannel channel, FileSmoosher smoosher) throws IOException;
  }

  public interface Deserializer
  {
    public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig);
  }
}
