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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.InputFormats.GenericFormatDefn;
import org.apache.druid.catalog.model.table.InputFormats.InputFormatDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@Category(CatalogTest.class)
public class InputFormatTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testCsvFormat()
  {
    InputFormatDefn converter = InputFormats.CSV_FORMAT_DEFN;
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );

    // Basic case
    {
      Map<String, Object> args = ImmutableMap.of(
          "listDelimiter", "|", "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      CsvInputFormat expectedFormat = new CsvInputFormat(
          Arrays.asList("x", "y"),
          "|",
          false,
          false,
          1
      );
      InputFormat inputFormat = converter.convert(table);
      assertEquals(expectedFormat, inputFormat);
    }

    // Minimal case. (However, though skipRows is required, JSON will handle
    // a null value and set the value to 0.)
    {
      Map<String, Object> args = ImmutableMap.of();
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      CsvInputFormat expectedFormat = new CsvInputFormat(
          Arrays.asList("x", "y"),
          null,
          false,
          false,
          0
      );
      InputFormat inputFormat = converter.convert(table);
      assertEquals(expectedFormat, inputFormat);
    }

    // Invalid format
    {
      Map<String, Object> args = ImmutableMap.of(
          "skipRows", "bogus"
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      assertThrows(Exception.class, () -> converter.convert(table));
    }

    // No columns
    {
      Map<String, Object> args = ImmutableMap.of(
          "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      assertThrows(Exception.class, () -> converter.convert(table));
    }
  }

  @Test
  public void testDelimitedTextFormat()
  {
    InputFormatDefn converter = InputFormats.DELIMITED_FORMAT_DEFN;
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );

    Map<String, Object> args = ImmutableMap.of(
        "delimiter", ",", "listDelimiter", "|", "skipRows", 1
    );
    TableSpec spec = new TableSpec("type", args, cols);
    ResolvedTable table = new ResolvedTable(null, spec, mapper);

    DelimitedInputFormat expectedFormat = new DelimitedInputFormat(
        Arrays.asList("x", "y"),
        "|",
        ",",
        false,
        false,
        1
    );
    InputFormat inputFormat = converter.convert(table);
    assertEquals(expectedFormat, inputFormat);
  }

  @Test
  public void testJsonFormat()
  {
    InputFormatDefn converter = InputFormats.JSON_FORMAT_DEFN;
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );

    // The one supported property at present.
    {
      Map<String, Object> args = ImmutableMap.of(
          "keepNulls", true
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      InputFormat inputFormat = converter.convert(table);
      assertEquals(new JsonInputFormat(null, null, true, null, null), inputFormat);
    }

    // Empty
    {
      TableSpec spec = new TableSpec("type", null, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      InputFormat inputFormat = converter.convert(table);
      assertEquals(new JsonInputFormat(null, null, null, null, null), inputFormat);
    }
  }

  /**
   * Test the generic format which allows a literal input spec. The
   * drawback is that the user must repeat the columns.
   */
  @Test
  public void testGenericFormat()
  {
    InputFormatDefn converter = InputFormats.GENERIC_FORMAT_DEFN;
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );

    // No type
    {
      Map<String, Object> args = ImmutableMap.of(
          "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      assertThrows(Exception.class, () -> converter.convert(table));
    }

    // CSV
    {
      Map<String, Object> args = ImmutableMap.of(
          GenericFormatDefn.INPUT_FORMAT_SPEC_PROPERTY,
          ImmutableMap.of(
              "type", CsvInputFormat.TYPE_KEY,
              "listDelimiter", "|",
              "skipHeaderRows", 1,
              "findColumnsFromHeader", false,
              "columns", Arrays.asList("x", "y")
          )
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      CsvInputFormat expectedFormat = new CsvInputFormat(
          Arrays.asList("x", "y"),
          "|",
          false,
          false,
          1
      );
      InputFormat inputFormat = converter.convert(table);
      assertEquals(expectedFormat, inputFormat);
    }

    // No columns: when using generic, the columns must be in the
    // JSON spec.
    {
      Map<String, Object> args = ImmutableMap.of(
          "type", CsvInputFormat.TYPE_KEY,
          "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      assertThrows(Exception.class, () -> converter.convert(table));
    }

    // Delimited text
    {
      Map<String, Object> args = ImmutableMap.of(
          GenericFormatDefn.INPUT_FORMAT_SPEC_PROPERTY,
          ImmutableMap.builder()
              .put("type", DelimitedInputFormat.TYPE_KEY)
              .put("delimiter", ",")
              .put("listDelimiter", "|")
              .put("skipHeaderRows", 1)
              .put("findColumnsFromHeader", false)
              .put("columns", Arrays.asList("x", "y"))
              .build()
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      DelimitedInputFormat expectedFormat = new DelimitedInputFormat(
          Arrays.asList("x", "y"),
          "|",
          ",",
          false,
          false,
          1
      );
      InputFormat inputFormat = converter.convert(table);
      assertEquals(expectedFormat, inputFormat);
    }

    // JSON
    {
      Map<String, Object> args = ImmutableMap.of(
          GenericFormatDefn.INPUT_FORMAT_SPEC_PROPERTY,
          ImmutableMap.of(
              "type", JsonInputFormat.TYPE_KEY,
              "keepNullColumns", true
          )
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      InputFormat inputFormat = converter.convert(table);
      assertEquals(new JsonInputFormat(null, null, true, null, null), inputFormat);
    }
  }
}
