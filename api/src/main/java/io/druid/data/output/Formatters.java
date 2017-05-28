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
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.logger.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;

/**
 */
public class Formatters
{
  private static final Logger log = new Logger(Formatter.class);

  public static Pair<Closeable, Accumulator> toExporter(
      Map<String, String> context,
      OutputStream output,
      ObjectMapper jsonMapper
  )
  {
      return toSimpleExporter(output, context, jsonMapper);
  }

  private static Pair<Closeable, Accumulator> toSimpleExporter(
      final OutputStream output,
      Map<String, String> context,
      ObjectMapper jsonMapper
  )
  {
    Closeable resource = new Closeable()
    {
      @Override
      public void close() throws IOException
      {
      }
    };
    final byte[] newLine = System.lineSeparator().getBytes();
    final Formatter formatter = getFormatter(context, jsonMapper);
    return Pair.<Closeable, Accumulator>of(
        resource, new Accumulator<Object, Map<String, Object>>()
        {
          @Override
          public Object accumulate(Object accumulated, Map<String, Object> in)
          {
            try {
              output.write(formatter.format(in));
              output.write(newLine);
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
            return null;
          }
        }
    );
  }

  private static Formatter getFormatter(Map<String, String> context, ObjectMapper jsonMapper)
  {
    String formatString = context.get("format");
    if (isNullOrEmpty(formatString) || formatString.equalsIgnoreCase("json")) {
      return new Formatter.JsonFormatter(jsonMapper);
    }
    if (formatString.equalsIgnoreCase("parquet")) {
      return new Formatter.ParquetFormatter();
    }
    String separator;
    if (formatString.equalsIgnoreCase("csv")) {
      separator = ",";
    } else if (formatString.equalsIgnoreCase("tsv")) {
      separator = "\t";
    } else {
      log.warn("Invalid format " + formatString + ".. using json formatter instead");
      return new Formatter.JsonFormatter(jsonMapper);
    }
    String nullValue = context.get("nullValue");
    String columns = context.get("columns");

    return new Formatter.XSVFormatter(separator, nullValue, toDimensionOrder(columns));
  }

  private static String[] toDimensionOrder(String columns)
  {
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
    return dimensions;
  }

  private static boolean isNullOrEmpty(String string)
  {
    return string == null || string.isEmpty();
  }
}
