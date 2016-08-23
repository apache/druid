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
import io.druid.java.util.common.logger.Logger;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

/**
 */
public class Formatters
{
  private static final Logger log = new Logger(Formatter.class);

  public static CountingAccumulator toExporter(
      Map<String, String> context,
      OutputStream output,
      ObjectMapper jsonMapper
  )
  {
    if ("excel".equals(context.get("format"))) {
      return toExcelExporter(output, context);
    } else {
      return toSimpleExporter(output, context, jsonMapper);
    }
  }

  private static CountingAccumulator toExcelExporter(
      final OutputStream output,
      Map<String, String> context
  )
  {
    final String[] dimensions = Formatters.toDimensionOrder(context.get("columns"));
    final HSSFWorkbook wb = new HSSFWorkbook();
    final Sheet sheet = wb.createSheet();

    final Closeable resource = new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        wb.write(output);
      }
    };
    if (dimensions != null) {
      Row r = sheet.createRow(0);
      for (int i = 0; i < dimensions.length; i++) {
        Cell c = r.createCell(i);
        c.setCellValue(dimensions[i]);
      }
      return new CountingAccumulator<Object, Map<String, Object>>()
      {
        private int rowNum = 1;

        @Override
        public int count()
        {
          return rowNum - 1;
        }

        @Override
        public Object accumulate(Object accumulated, Map<String, Object> in)
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
          resource.close();
        }
      };
    }
    return new CountingAccumulator<Object, Map<String, Object>>()
    {
      private int rowNum = 0;

      @Override
      public int count()
      {
        return rowNum;
      }

      @Override
      public Object accumulate(Object accumulated, Map<String, Object> in)
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
        resource.close();
      }
    };
  }

  private static CountingAccumulator toSimpleExporter(
      final OutputStream output,
      Map<String, String> context,
      ObjectMapper jsonMapper
  )
  {
    final byte[] newLine = System.lineSeparator().getBytes();
    final Formatter formatter = getFormatter(context, jsonMapper);
    return new CountingAccumulator<Object, Map<String, Object>>()
    {
      int counter = 0;

      @Override
      public int count()
      {
        return counter;
      }

      @Override
      public Object accumulate(Object accumulated, Map<String, Object> in)
      {
        try {
          output.write(formatter.format(in));
          output.write(newLine);
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
      }
    };
  }

  private static Formatter getFormatter(Map<String, String> context, ObjectMapper jsonMapper)
  {
    String formatString = context.get("format");
    if (isNullOrEmpty(formatString) || formatString.equalsIgnoreCase("json")) {
      return new Formatter.JsonFormatter(jsonMapper);
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
