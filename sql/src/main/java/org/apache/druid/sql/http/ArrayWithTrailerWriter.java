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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.context.ResponseContext;

import java.io.IOException;
import java.io.OutputStream;

public class ArrayWithTrailerWriter extends ArrayWriter
{
  private boolean wroteTrailer;

  public ArrayWithTrailerWriter(final OutputStream outputStream, final ObjectMapper jsonMapper) throws IOException
  {
    super(outputStream, jsonMapper);
  }

  @Override
  public void writeResponseStart() throws IOException
  {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeFieldName(JsonParserIterator.FIELD_RESULTS);
    jsonGenerator.writeStartArray();
  }

  @Override
  public void writeTrailer(final ResponseContext context) throws IOException
  {
    if (wroteTrailer) {
      throw new ISE("Cannot write trailer more than once");
    }

    // Assumes the caller has included only the desired fields in the context
    jsonGenerator.writeEndArray();
    jsonGenerator.writeObjectField(JsonParserIterator.FIELD_CONTEXT, context.toMap());
    jsonGenerator.writeEndObject();
    wroteTrailer = true;
  }

  @Override
  public void writeResponseEnd() throws IOException
  {
    if (!wroteTrailer) {
      throw new ISE("Expected trailer before response end");
    }

    // End with LF.
    jsonGenerator.flush();
    outputStream.write('\n');
  }
}
