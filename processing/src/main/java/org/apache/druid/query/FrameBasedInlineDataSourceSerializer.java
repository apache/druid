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

package org.apache.druid.query;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Serializes {@link FrameBasedInlineDataSource} to the representation of {@link InlineDataSource}
 * so that the servers' on wire transfer data doesn't change. {@link FrameBasedInlineDataSource} is currently limited
 * to the brokers only and therefore this aids in conversion of the object to a representation that the data servers
 * can recognize
 */
public class FrameBasedInlineDataSourceSerializer extends StdSerializer<FrameBasedInlineDataSource>
{
  public FrameBasedInlineDataSourceSerializer()
  {
    super(FrameBasedInlineDataSource.class);
  }

  @Override
  public void serialize(FrameBasedInlineDataSource value, JsonGenerator jg, SerializerProvider serializers)
      throws IOException
  {
    jg.writeStartObject();
    jg.writeStringField("type", "inline");

    RowSignature rowSignature = value.getRowSignature();
    jg.writeObjectField("columnNames", rowSignature.getColumnNames());
    List<ColumnType> columnTypes = IntStream.range(0, rowSignature.size())
                                            .mapToObj(i -> rowSignature.getColumnType(i).orElse(null))
                                            .collect(Collectors.toList());
    jg.writeObjectField("columnTypes", columnTypes);

    jg.writeArrayFieldStart("rows");

    value.getRowsAsSequence().forEach(row -> {
      try {
        JacksonUtils.writeObjectUsingSerializerProvider(jg, serializers, row);
      }
      catch (IOException e) {
        // Ideally, this shouldn't be reachable.
        // Wrap the IO exception in the runtime exception and propogate it forward
        List<String> elements = new ArrayList<>();
        for (Object o : row) {
          elements.add(o.toString());
        }
        throw new RE(
            e,
            "Exception encountered while serializing [%s] in [%s]",
            String.join(", ", elements),
            FrameBasedInlineDataSource.class
        );
      }
    });

    jg.writeEndArray();
    jg.writeEndObject();
  }

  /**
   * Required because {@link DataSource} is polymorphic
   */
  @Override
  public void serializeWithType(
      FrameBasedInlineDataSource value,
      JsonGenerator jg,
      SerializerProvider serializers,
      TypeSerializer typeSer
  ) throws IOException
  {
    serialize(value, jg, serializers);
  }
}
