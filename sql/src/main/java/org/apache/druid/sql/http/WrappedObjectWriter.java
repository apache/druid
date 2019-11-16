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
import org.apache.druid.sql.http.ResultFormat.Writer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class WrappedObjectWriter implements Writer
{

  private final ObjectWriter objectWriter;
  private final JsonGenerator jsonGenerator;
  private final OutputStream outputStream;

  public WrappedObjectWriter(ObjectWriter objectWriter)
  {
    this.objectWriter = objectWriter;
    this.jsonGenerator = objectWriter.getJsonGenerator();
    this.outputStream = objectWriter.getOutputStream();
  }

  @Override
  public void writeResponseStart() throws IOException
  {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeFieldName("data");
    jsonGenerator.writeStartArray();
  }

  @Override
  public void writeHeader(List<String> columnNames) throws IOException
  {
    objectWriter.writeHeader(columnNames);
  }

  @Override
  public void writeRowStart() throws IOException
  {
    objectWriter.writeRowStart();
  }

  @Override
  public void writeRowField(String name, Object value) throws IOException
  {
    objectWriter.writeRowField(name, value);
  }

  @Override
  public void writeRowEnd() throws IOException
  {
    objectWriter.writeRowEnd();
  }

  @Override
  public void writeResponseEnd() throws IOException
  {
    jsonGenerator.writeEndArray();
    jsonGenerator.writeEndObject();
    jsonGenerator.flush();
    outputStream.write('\n');
  }

  @Override
  public void close() throws IOException
  {
    objectWriter.close();
  }

}
