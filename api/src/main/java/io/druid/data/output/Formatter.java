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

package io.druid.data.output;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 */
public interface Formatter
{
  byte[] NEW_LINE = System.lineSeparator().getBytes();

  void begin(OutputStream output) throws IOException;

  void write(OutputStream output, Map<String, Object> datum) throws IOException;

  void end(OutputStream output) throws IOException;

  class XSVFormatter implements Formatter
  {
    private final String separator;
    private final String nullValue;
    private final String[] dimensions;

    private final StringBuilder builder = new StringBuilder();

    public XSVFormatter(String separator)
    {
      this(separator, null, null);
    }

    public XSVFormatter(String separator, String nullValue, String[] dimensions)
    {
      this.separator = separator == null ? "," : separator;
      this.nullValue = nullValue == null ? "NULL" : nullValue;
      this.dimensions = dimensions;
    }

    @Override
    public void begin(OutputStream output) throws IOException
    {
    }

    @Override
    public void write(OutputStream output, Map<String, Object> datum) throws IOException
    {
      builder.setLength(0);

      if (dimensions == null) {
        for (Object value : datum.values()) {
          if (builder.length() > 0) {
            builder.append(separator);
          }
          builder.append(value == null ? nullValue : String.valueOf(value));
        }
      } else {
        for (String dimension : dimensions) {
          Object value = datum.get(dimension);
          if (builder.length() > 0) {
            builder.append(separator);
          }
          builder.append(value == null ? nullValue : String.valueOf(value));
        }
      }
      output.write(builder.toString().getBytes());
    }

    @Override
    public void end(OutputStream output) throws IOException
    {
    }
  }

  class JsonFormatter implements Formatter
  {
    private static final byte[] HEAD = ("[" + System.lineSeparator()).getBytes();
    private static final byte[] NEXT_LINE = (", " + System.lineSeparator()).getBytes();
    private static final byte[] TAIL = (System.lineSeparator() + "]" + System.lineSeparator()).getBytes();

    private final ObjectMapper jsonMapper;
    private final boolean withWrapping;

    private boolean firstLine;

    public JsonFormatter(ObjectMapper jsonMapper, boolean withWrapping)
    {
      this.jsonMapper = jsonMapper;
      this.withWrapping = withWrapping;
    }

    @Override
    public void begin(OutputStream output) throws IOException
    {
      if (withWrapping) {
        output.write(HEAD);
      }
      firstLine = true;
    }

    @Override
    public void write(OutputStream output, Map<String, Object> datum) throws IOException
    {
      if (withWrapping && !firstLine) {
        output.write(NEXT_LINE);
      }
      // jsonMapper.writeValue(output, datum) closes stream
      output.write(jsonMapper.writeValueAsBytes(datum));
      firstLine = false;
    }

    @Override
    public void end(OutputStream output) throws IOException
    {
      if (withWrapping) {
        output.write(TAIL);
      }
    }
  }
}
