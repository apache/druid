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
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;

public class ArrayLinesWriter implements ResultFormat.Writer
{
  private final SerializerProvider serializers;
  private final JsonGenerator jsonGenerator;
  private final OutputStream outputStream;

  public ArrayLinesWriter(final OutputStream outputStream, final ObjectMapper jsonMapper) throws IOException
  {
    this.serializers = jsonMapper.getSerializerProviderInstance();
    this.outputStream = outputStream;
    this.jsonGenerator = jsonMapper.writer().getFactory().createGenerator(outputStream);
    jsonGenerator.setRootValueSeparator(new SerializedString("\n"));
  }

  @Override
  public void writeResponseStart()
  {
    // Do nothing.
  }

  @Override
  public void writeResponseEnd() throws IOException
  {
    jsonGenerator.flush();

    // Terminate the last output line, then write an extra blank line, so users can tell the response was not cut off.
    outputStream.write(new byte[]{'\n', '\n'});
    outputStream.flush();
  }

  @Override
  public void writeHeader(
      final RelDataType rowType,
      final boolean includeTypes,
      final boolean includeSqlTypes
  ) throws IOException
  {
    ArrayWriter.writeHeader(jsonGenerator, rowType, includeTypes, includeSqlTypes);
  }

  @Override
  public void writeHeaderFromRowSignature(
      final RowSignature rowSignature,
      final boolean includeTypes
  ) throws IOException
  {
    ArrayWriter.writeHeader(jsonGenerator, rowSignature, includeTypes);
  }

  @Override
  public void writeRowStart() throws IOException
  {
    jsonGenerator.writeStartArray();
  }

  @Override
  public void writeRowField(final String name, @Nullable final Object value) throws IOException
  {
    JacksonUtils.writeObjectUsingSerializerProvider(jsonGenerator, serializers, value);
  }

  @Override
  public void writeRowEnd() throws IOException
  {
    jsonGenerator.writeEndArray();
  }

  @Override
  public void close() throws IOException
  {
    jsonGenerator.close();
  }
}
