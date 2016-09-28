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
import com.google.common.io.ByteSink;
import io.druid.java.util.common.logger.Logger;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 */
public class Formatters
{
  private static final Logger log = new Logger(Formatter.class);

  public static CountingAccumulator toBasicExporter(
      Map<String, Object> context,
      ObjectMapper jsonMapper,
      ByteSink output
  ) throws IOException
  {
    if ("excel".equals(Objects.toString(context.get("format"), null))) {
      return toExcelExporter(output, context);
    } else {
      return wrapToExporter(toBasicFormatter(output, context, jsonMapper));
    }
  }

  public static CountingAccumulator toExcelExporter(final ByteSink sink, final Map<String, Object> context)
      throws IOException
  {
    final String[] dimensions = Formatters.toDimensionOrder(Objects.toString(context.get("columns"), null));
    final HSSFWorkbook wb = new HSSFWorkbook();
    final Sheet sheet = wb.createSheet();

    if (dimensions != null) {
      return new CountingAccumulator()
      {
        private int rowNum;

        @Override
        public int count()
        {
          return rowNum - 1;
        }

        @Override
        public void init() throws IOException
        {
          Row r = sheet.createRow(rowNum++);
          for (int i = 0; i < dimensions.length; i++) {
            Cell c = r.createCell(i);
            c.setCellValue(dimensions[i]);
          }
        }

        @Override
        public Void accumulate(Void accumulated, Map<String, Object> in)
        {
          Row r = sheet.createRow(rowNum++);
          for (int i = 0; i < dimensions.length; i++) {
            Cell c = r.createCell(i);
            Object o = in.get(dimensions[i]);
            if (o instanceof Number) {
              c.setCellValue(((Number) o).doubleValue());
            } else if (o instanceof String) {
              c.setCellValue((String) o);
            } else if (o instanceof Date) {
              c.setCellValue((Date) o);
            } else {
              c.setCellValue(String.valueOf(o));
            }
          }
          return null;
        }

        @Override
        public void close() throws IOException
        {
          try (OutputStream output = sink.openBufferedStream()) {
            wb.write(output);
          }
        }
      };
    }
    return new CountingAccumulator()
    {
      private int rowNum;

      @Override
      public int count()
      {
        return rowNum;
      }

      @Override
      public void init() throws IOException
      {
      }

      @Override
      public Void accumulate(Void accumulated, Map<String, Object> in)
      {
        Row r = sheet.createRow(rowNum++);
        int i = 0;
        for (Object o : in.values()) {
          Cell c = r.createCell(i++);
          if (o instanceof Number) {
            c.setCellValue(((Number) o).doubleValue());
          } else if (o instanceof String) {
            c.setCellValue((String) o);
          } else if (o instanceof Date) {
            c.setCellValue((Date) o);
          } else {
            c.setCellValue(String.valueOf(o));
          }
        }
        return null;
      }

      @Override
      public void close() throws IOException
      {
        try (OutputStream output = sink.openBufferedStream()) {
          wb.write(output);
        }
      }
    };
  }

  public static CountingAccumulator wrapToExporter(final Formatter formatter)
  {
    return new CountingAccumulator()
    {
      int counter = 0;

      @Override
      public int count()
      {
        return counter;
      }

      @Override
      public void init() throws IOException
      {
      }

      @Override
      public Void accumulate(Void accumulated, Map<String, Object> in)
      {
        try {
          formatter.write(in);
          counter++;
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        return null;
      }

      @Override
      public void close() throws IOException
      {
        formatter.close();
      }
    };
  }

  public static Formatter toBasicFormatter(ByteSink output, Map<String, Object> context, ObjectMapper jsonMapper)
      throws IOException
  {
    String formatString = Objects.toString(context.get("format"), null);
    if (isNullOrEmpty(formatString) || formatString.equalsIgnoreCase("json")) {
      boolean wrapAsList = parseBoolean(context.get("wrapAsList"), false);
      return new Formatter.JsonFormatter(output.openBufferedStream(), jsonMapper, wrapAsList);
    }
    String separator;
    if (formatString.equalsIgnoreCase("csv")) {
      separator = ",";
    } else if (formatString.equalsIgnoreCase("tsv")) {
      separator = "\t";
    } else {
      log.warn("Invalid format " + formatString + ".. using json formatter instead");
      boolean wrapAsList = parseBoolean(context.get("wrapAsList"), false);
      return new Formatter.JsonFormatter(output.openBufferedStream(), jsonMapper, wrapAsList);
    }
    String nullValue = Objects.toString(context.get("nullValue"), null);
    String columns = Objects.toString(context.get("columns"), null);

    return new Formatter.XSVFormatter(output.openBufferedStream(), separator, nullValue, toDimensionOrder(columns));
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

  private static boolean parseBoolean(Object input, boolean defaultValue)
  {
    return input == null ? defaultValue :
           input instanceof Boolean ? (Boolean)input : Boolean.valueOf(String.valueOf(input));
  }
}
