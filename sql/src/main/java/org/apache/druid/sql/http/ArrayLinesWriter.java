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

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class ArrayLinesWriter implements ResultFormat.Writer
{
  private final OutputStream outputStream;
  private final JsonGenerator jsonGenerator;

  public ArrayLinesWriter(final OutputStream outputStream, final ObjectMapper jsonMapper) throws IOException
  {
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
  public void writeHeader(final List<String> columnNames) throws IOException
  {
    jsonGenerator.writeStartArray();

    for (String columnName : columnNames) {
      jsonGenerator.writeString(columnName);
    }

    jsonGenerator.writeEndArray();
  }

  @Override
  public void writeRowStart() throws IOException
  {
    jsonGenerator.writeStartArray();
  }

  @Override
  public void writeRowField(final String name, @Nullable final Object value) throws IOException
  {
    jsonGenerator.writeObject(value);
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
