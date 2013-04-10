/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;

import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Map;

/**
 */
public class TsvToJson
{
  public static void main(String[] args) throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();

    String[] fields = args[0].split(",");
    File inFile = new File(args[1]);
    File outFile = new File(args[2]);

    FieldHandler[] handlers = new FieldHandler[fields.length];
    for (int i = 0; i < fields.length; i++) {
      String field = fields[i];
      String[] fieldParts = field.split(":");
      String fieldName = fieldParts[0];
      if (fieldParts.length < 2 || "string".equalsIgnoreCase(fieldParts[1])) {
        handlers[i] = new StringField(fieldName);
      }
      else if ("number".equalsIgnoreCase(fieldParts[1])) {
        handlers[i] = new NumberField(fieldName);
      }
      else if ("ISO8601".equals(fieldParts[1])) {
        handlers[i] = new IsoToNumberField(fieldName);
      }
      else {
        throw new IAE("Unknown type[%s]", fieldParts[1]);
      }
    }

    BufferedReader in = null;
    BufferedWriter out = null;
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(inFile), Charsets.UTF_8));
      out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile), Charsets.UTF_8));
      String line = null;
      int count = 0;
      long currTime = System.currentTimeMillis();
      long startTime = currTime;
      while ((line = in.readLine()) != null) {
        if (count % 1000000 == 0) {
          long nowTime = System.currentTimeMillis();
          System.out.printf("Processed [%,d] lines in %,d millis.  Incremental time %,d millis.%n", count, nowTime - startTime, nowTime - currTime);
          currTime = nowTime;
        }
        ++count;
        String[] splits = line.split("\t");

        if (splits.length == 30) {
          continue;
        }

        if (splits.length != handlers.length) {
          throw new IAE("splits.length[%d] != handlers.length[%d]; line[%s]", splits.length, handlers.length, line);
        }

        Map<String, Object> jsonMap = Maps.newLinkedHashMap();
        for (int i = 0; i < handlers.length; ++i) {
          jsonMap.put(handlers[i].getFieldName(), handlers[i].process(splits[i]));
        }

        final String str = mapper.writeValueAsString(jsonMap);
        out.write(str);
        out.write("\n");
      }
      System.out.printf("Completed %,d lines in %,d millis.%n", count, System.currentTimeMillis() - startTime);
      out.flush();
    } finally {
      if (out != null) {
        out.close();
      }
      if (in != null) {
        in.close();
      }
    }
  }

  public static interface FieldHandler
  {
    public String getFieldName();
    public Object process(String value);
  }

  public static class StringField implements FieldHandler
  {
    private final String fieldName;

    public StringField(
        String fieldName
    )
    {
      this.fieldName = fieldName;
    }

    @Override
    public String getFieldName()
    {
      return fieldName;
    }

    @Override
    public Object process(String value)
    {
      return value;
    }
  }

  public static class NumberField implements FieldHandler
  {
    private final String fieldName;

    public NumberField(
        String fieldName
    )
    {
      this.fieldName = fieldName;
    }

    @Override
    public String getFieldName()
    {
      return fieldName;
    }

    @Override
    public Object process(String value)
    {
      try {
        return Long.parseLong(value);
      } catch (NumberFormatException e) {
        return Double.parseDouble(value);
      }
    }
  }

  public static class IsoToNumberField implements FieldHandler
  {

    private final String fieldName;

    public IsoToNumberField(
        String fieldName
    )
    {
      this.fieldName = fieldName;
    }

    @Override
    public String getFieldName()
    {
      return fieldName;
    }

    @Override
    public Object process(String value)
    {
      return new DateTime(value).getMillis();
    }
  }
}
