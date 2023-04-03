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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FramesBackedInlineDataSourceSerializer extends StdSerializer<FramesBackedInlineDataSource>
{
  public FramesBackedInlineDataSourceSerializer()
  {
    super(FramesBackedInlineDataSource.class);
  }

  @Override
  public void serialize(FramesBackedInlineDataSource value, JsonGenerator jg, SerializerProvider serializers)
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
        jg.writeStartArray();
        for (Object o : row) {
          jg.writeObject(o);
        }
        jg.writeEndArray();
      }
      catch (IOException e) {
        // Do nothing, shouldn't be possible for now
      }
    });

    jg.writeEndArray();
    jg.writeEndObject();
  }

  @Override
  public void serializeWithType(
      FramesBackedInlineDataSource value,
      JsonGenerator jg,
      SerializerProvider serializers,
      TypeSerializer typeSer
  ) throws IOException
  {
    serialize(value, jg, serializers);
  }
}
