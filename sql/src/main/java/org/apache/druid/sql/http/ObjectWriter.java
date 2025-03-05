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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;

public class ObjectWriter implements ResultFormat.Writer
{
  static final String TYPE_HEADER_NAME = "type";
  static final String SQL_TYPE_HEADER_NAME = "sqlType";

  private final SerializerProvider serializers;
  private final JsonGenerator jsonGenerator;
  private final OutputStream outputStream;

  public ObjectWriter(final OutputStream outputStream, final ObjectMapper jsonMapper) throws IOException
  {
    this.serializers = jsonMapper.getSerializerProviderInstance();
    this.jsonGenerator = jsonMapper.getFactory().createGenerator(outputStream);
    this.outputStream = outputStream;
  }

  @Override
  public void writeResponseStart() throws IOException
  {
    jsonGenerator.writeStartArray();
  }

  @Override
  public void writeResponseEnd() throws IOException
  {
    jsonGenerator.writeEndArray();

    // End with LF.
    jsonGenerator.flush();
    outputStream.write('\n');
  }

  @Override
  public void writeHeader(
      final RelDataType rowType,
      final boolean includeTypes,
      final boolean includeSqlTypes
  ) throws IOException
  {
    writeHeader(jsonGenerator, rowType, includeTypes, includeSqlTypes);
  }

  @Override
  public void writeHeaderFromRowSignature(final RowSignature signature, final boolean includeTypes) throws IOException
  {
    writeHeader(jsonGenerator, signature, includeTypes);
  }

  @Override
  public void writeRowStart() throws IOException
  {
    jsonGenerator.writeStartObject();
  }

  @Override
  public void writeRowField(final String name, @Nullable final Object value) throws IOException
  {
    jsonGenerator.writeFieldName(name);
    JacksonUtils.writeObjectUsingSerializerProvider(jsonGenerator, serializers, value);
  }

  @Override
  public void writeRowEnd() throws IOException
  {
    jsonGenerator.writeEndObject();
  }

  @Override
  public void close() throws IOException
  {
    jsonGenerator.close();
  }

  static void writeHeader(
      final JsonGenerator jsonGenerator,
      final RelDataType rowType,
      final boolean includeTypes,
      final boolean includeSqlTypes
  ) throws IOException
  {
    final RowSignature signature = RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType);

    jsonGenerator.writeStartObject();

    for (int i = 0; i < signature.size(); i++) {
      jsonGenerator.writeFieldName(signature.getColumnName(i));

      if (!includeTypes && !includeSqlTypes) {
        jsonGenerator.writeNull();
      } else {
        jsonGenerator.writeStartObject();

        if (includeTypes) {
          jsonGenerator.writeStringField(
              ObjectWriter.TYPE_HEADER_NAME,
              signature.getColumnType(i).map(TypeSignature::asTypeString).orElse(null)
          );
        }

        if (includeSqlTypes) {
          jsonGenerator.writeStringField(
              ObjectWriter.SQL_TYPE_HEADER_NAME,
              rowType.getFieldList().get(i).getType().getSqlTypeName().getName()
          );
        }

        jsonGenerator.writeEndObject();
      }
    }

    jsonGenerator.writeEndObject();
  }

  static void writeHeader(
      final JsonGenerator jsonGenerator,
      final RowSignature signature,
      final boolean includeTypes
  ) throws IOException
  {
    jsonGenerator.writeStartObject();

    for (int i = 0; i < signature.size(); i++) {
      jsonGenerator.writeFieldName(signature.getColumnName(i));

      if (!includeTypes) {
        jsonGenerator.writeNull();
      } else {
        jsonGenerator.writeStartObject();

        jsonGenerator.writeStringField(
            ObjectWriter.TYPE_HEADER_NAME,
            signature.getColumnType(i).map(TypeSignature::asTypeString).orElse(null)
        );

        jsonGenerator.writeEndObject();
      }
    }

    jsonGenerator.writeEndObject();
  }
}
