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
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metamx.common.logger.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 */
public class Formatters
{
  private static final Logger log = new Logger(Formatter.class);

  public static Formatter getFormatter(Map<String, String> context, ObjectMapper jsonMapper)
  {
    String formatString = context.get("format");
    if (isNullOrEmpty(formatString) || formatString.equalsIgnoreCase("json")) {
      return new JsonFormatter(jsonMapper);
    }
    String separator;
    if (formatString.equalsIgnoreCase("csv")) {
      separator = ",";
    } else if (formatString.equalsIgnoreCase("tsv")) {
      separator = "\t";
    } else {
      log.warn("Invalid format " + formatString + ".. using json formatter instead");
      return new JsonFormatter(jsonMapper);
    }
    String nullValue = context.get("nullValue");
    String columns = context.get("columns");

    String[] dimensions = null;
    if (!isNullOrEmpty(columns)) {
      dimensions = Iterables.toArray(
          Iterables.transform(
              Arrays.asList(columns.split(",")), new Function<String, String>()
              {
                @Override
                public String apply(String input)
                {
                  return input.trim();
                }
              }
          ),
          String.class
      );
    }

    return new XSVFormatter(separator, nullValue, dimensions);
  }

  private static boolean isNullOrEmpty(String string)
  {
    return string == null || string.isEmpty();
  }

  static class XSVFormatter implements Formatter
  {
    private final String separator;
    private final String nullValue;
    private final String[] dimensions;

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
    public byte[] format(Map<String, Object> datum) throws IOException
    {
      StringBuilder builder = new StringBuilder();
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
      return builder.toString().getBytes();
    }
  }

  static class JsonFormatter implements Formatter
  {
    private ObjectMapper jsonMapper;

    public JsonFormatter(ObjectMapper jsonMapper)
    {
      this.jsonMapper = jsonMapper;
    }

    @Override
    public byte[] format(Map<String, Object> datum) throws IOException
    {
      return jsonMapper.writeValueAsBytes(datum);
    }
  }
}
