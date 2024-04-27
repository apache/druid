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

package org.apache.druid.data.input.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FileEntity;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.transform.TransformingInputEntityReader;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class OrcReaderTest extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  // This test is migrated from OrcHadoopInputRowParserTest
  @Test
  public void testTest1() throws IOException
  {
    final InputEntityReader reader = createReader(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("col1", "col2"))),
        new OrcInputFormat(null, null, new Configuration()),
        "example/test_1.orc"
    );
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      Assert.assertEquals(DateTimes.of("2016-01-01T00:00:00.000Z"), row.getTimestamp());
      Assert.assertEquals("bar", Iterables.getOnlyElement(row.getDimension("col1")));
      Assert.assertEquals(ImmutableList.of("dat1", "dat2", "dat3"), row.getDimension("col2"));
      Assert.assertEquals(1.1, row.getMetric("val1").doubleValue(), 0.001);
      Assert.assertFalse(iterator.hasNext());
    }
  }

  // This test is migrated from OrcHadoopInputRowParserTest
  @Test
  public void testTest2() throws IOException
  {
    final InputFormat inputFormat = new OrcInputFormat(
        new JSONPathSpec(
            true,
            Collections.singletonList(new JSONPathFieldSpec(JSONPathFieldType.PATH, "col7-subcol7", "$.col7.subcol7"))
        ),
        null,
        new Configuration()
    );
    final InputEntityReader reader = createReader(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(null),
        inputFormat,
        "example/test_2.orc"
    );
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      Assert.assertEquals(DateTimes.of("2016-01-01T00:00:00.000Z"), row.getTimestamp());
      Assert.assertEquals("bar", Iterables.getOnlyElement(row.getDimension("col1")));
      Assert.assertEquals(ImmutableList.of("dat1", "dat2", "dat3"), row.getDimension("col2"));
      Assert.assertEquals("1.1", Iterables.getOnlyElement(row.getDimension("col3")));
      Assert.assertEquals("2", Iterables.getOnlyElement(row.getDimension("col4")));
      Assert.assertEquals("3.5", Iterables.getOnlyElement(row.getDimension("col5")));
      Assert.assertTrue(row.getDimension("col6").isEmpty());
      Assert.assertFalse(iterator.hasNext());
    }
  }

  // This test is migrated from OrcHadoopInputRowParserTest
  @Test
  public void testOrcFile11Format() throws IOException
  {
    final OrcInputFormat inputFormat = new OrcInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "struct_list_struct_int", "$.middle.list[1].int1"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "struct_list_struct_intlist", "$.middle.list[*].int1"),
                new JSONPathFieldSpec(
                    JSONPathFieldType.PATH,
                    "struct_list_struct_middleListLength",
                    "$.middle.list.length()"
                ),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "list_struct_string", "$.list[0].string1"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "map_struct_int", "$.map.chani.int1")
            )
        ),
        null,
        new Configuration()
    );
    final InputEntityReader reader = createReader(
        new TimestampSpec("ts", "millis", null),
        new DimensionsSpec(null),
        inputFormat,
        "example/orc-file-11-format.orc"
    );
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int actualRowCount = 0;

      // Check the first row
      Assert.assertTrue(iterator.hasNext());
      InputRow row = iterator.next();
      actualRowCount++;
      Assert.assertEquals("false", Iterables.getOnlyElement(row.getDimension("boolean1")));
      Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("byte1")));
      Assert.assertEquals("1024", Iterables.getOnlyElement(row.getDimension("short1")));
      Assert.assertEquals("65536", Iterables.getOnlyElement(row.getDimension("int1")));
      Assert.assertEquals("9223372036854775807", Iterables.getOnlyElement(row.getDimension("long1")));
      Assert.assertEquals("1.0", Iterables.getOnlyElement(row.getDimension("float1")));
      Assert.assertEquals("-15.0", Iterables.getOnlyElement(row.getDimension("double1")));
      Assert.assertEquals("AAECAwQAAA==", Iterables.getOnlyElement(row.getDimension("bytes1")));
      Assert.assertEquals("hi", Iterables.getOnlyElement(row.getDimension("string1")));
      Assert.assertEquals("1.23456786547456E7", Iterables.getOnlyElement(row.getDimension("decimal1")));
      Assert.assertEquals("2", Iterables.getOnlyElement(row.getDimension("struct_list_struct_int")));
      Assert.assertEquals(ImmutableList.of("1", "2"), row.getDimension("struct_list_struct_intlist"));
      Assert.assertEquals("good", Iterables.getOnlyElement(row.getDimension("list_struct_string")));

      Assert.assertEquals("2", Iterables.getOnlyElement(row.getDimension("struct_list_struct_middleListLength")));
      Assert.assertEquals(DateTimes.of("2000-03-12T15:00:00.0Z"), row.getTimestamp());

      while (iterator.hasNext()) {
        actualRowCount++;
        row = iterator.next();
      }

      // Check the last row
      Assert.assertEquals("true", Iterables.getOnlyElement(row.getDimension("boolean1")));
      Assert.assertEquals("100", Iterables.getOnlyElement(row.getDimension("byte1")));
      Assert.assertEquals("2048", Iterables.getOnlyElement(row.getDimension("short1")));
      Assert.assertEquals("65536", Iterables.getOnlyElement(row.getDimension("int1")));
      Assert.assertEquals("9223372036854775807", Iterables.getOnlyElement(row.getDimension("long1")));
      Assert.assertEquals("2.0", Iterables.getOnlyElement(row.getDimension("float1")));
      Assert.assertEquals("-5.0", Iterables.getOnlyElement(row.getDimension("double1")));
      Assert.assertEquals("", Iterables.getOnlyElement(row.getDimension("bytes1")));
      Assert.assertEquals("bye", Iterables.getOnlyElement(row.getDimension("string1")));
      Assert.assertEquals("1.23456786547457E7", Iterables.getOnlyElement(row.getDimension("decimal1")));
      Assert.assertEquals("2", Iterables.getOnlyElement(row.getDimension("struct_list_struct_int")));
      Assert.assertEquals(ImmutableList.of("1", "2"), row.getDimension("struct_list_struct_intlist"));
      Assert.assertEquals("cat", Iterables.getOnlyElement(row.getDimension("list_struct_string")));
      Assert.assertEquals("5", Iterables.getOnlyElement(row.getDimension("map_struct_int")));
      Assert.assertEquals(DateTimes.of("2000-03-12T15:00:01.000Z"), row.getTimestamp());

      Assert.assertEquals(7500, actualRowCount);
    }
  }

  // This test is migrated from OrcHadoopInputRowParserTest
  @Test
  public void testOrcSplitElim() throws IOException
  {
    final InputEntityReader reader = createReader(
        new TimestampSpec("ts", "millis", null),
        new DimensionsSpec(null),
        new OrcInputFormat(new JSONPathSpec(true, null), null, new Configuration()),
        "example/orc_split_elim.orc"
    );
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int actualRowCount = 0;
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      actualRowCount++;
      Assert.assertEquals(DateTimes.of("1969-12-31T16:00:00.0Z"), row.getTimestamp());
      Assert.assertEquals("2", Iterables.getOnlyElement(row.getDimension("userid")));
      Assert.assertEquals("foo", Iterables.getOnlyElement(row.getDimension("string1")));
      Assert.assertEquals("0.8", Iterables.getOnlyElement(row.getDimension("subtype")));
      Assert.assertEquals("1.2", Iterables.getOnlyElement(row.getDimension("decimal1")));
      while (iterator.hasNext()) {
        actualRowCount++;
        iterator.next();
      }
      Assert.assertEquals(25000, actualRowCount);
    }
  }

  // This test is migrated from OrcHadoopInputRowParserTest
  @Test
  public void testDate1900() throws IOException
  {
    final InputEntityReader reader = createReader(
        new TimestampSpec("time", "millis", null),
        DimensionsSpec.builder().setDimensionExclusions(Collections.singletonList("time")).build(),
        new OrcInputFormat(new JSONPathSpec(true, null), null, new Configuration()),
        "example/TestOrcFile.testDate1900.orc"
    );
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int actualRowCount = 0;
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      actualRowCount++;
      Assert.assertEquals(1, row.getDimensions().size());
      Assert.assertEquals(DateTimes.of("1900-05-05T12:34:56.1Z"), row.getTimestamp());
      Assert.assertEquals("1900-12-25T00:00:00.000Z", Iterables.getOnlyElement(row.getDimension("date")));
      while (iterator.hasNext()) {
        actualRowCount++;
        iterator.next();
      }
      Assert.assertEquals(70000, actualRowCount);
    }
  }

  // This test is migrated from OrcHadoopInputRowParserTest
  @Test
  public void testDate2038() throws IOException
  {
    final InputEntityReader reader = createReader(
        new TimestampSpec("time", "millis", null),
        DimensionsSpec.builder().setDimensionExclusions(Collections.singletonList("time")).build(),
        new OrcInputFormat(new JSONPathSpec(true, null), null, new Configuration()),
        "example/TestOrcFile.testDate2038.orc"
    );
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int actualRowCount = 0;
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      actualRowCount++;
      Assert.assertEquals(1, row.getDimensions().size());
      Assert.assertEquals(DateTimes.of("2038-05-05T12:34:56.1Z"), row.getTimestamp());
      Assert.assertEquals("2038-12-25T00:00:00.000Z", Iterables.getOnlyElement(row.getDimension("date")));
      while (iterator.hasNext()) {
        actualRowCount++;
        iterator.next();
      }
      Assert.assertEquals(212000, actualRowCount);
    }
  }

  /**
   * schema: struct<string1:string, list:array<int>, ts:timestamp>
   * data:   {"dim1","[7,8,9]","2000-03-12 15:00:00"}
   */
  @Test
  public void testJsonPathFunctions() throws IOException
  {
    final OrcInputFormat inputFormat = new OrcInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "min", "$.list.min()"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "max", "$.list.max()"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "avg", "$.list.avg()"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "len", "$.list.length()"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "sum", "$.list.sum()"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "stddev", "$.list.stddev()"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "append", "$.list.append(10)")
            )
        ),
        null,
        new Configuration()
    );
    final InputEntityReader reader = createReader(
        new TimestampSpec("ts", "millis", null),
        new DimensionsSpec(null),
        inputFormat,
        "example/test_json_path_functions.orc"
    );
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int actualRowCount = 0;

      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        actualRowCount++;

        Assert.assertEquals("7.0", Iterables.getOnlyElement(row.getDimension("min")));
        Assert.assertEquals("8.0", Iterables.getOnlyElement(row.getDimension("avg")));
        Assert.assertEquals("9.0", Iterables.getOnlyElement(row.getDimension("max")));
        Assert.assertEquals("24.0", Iterables.getOnlyElement(row.getDimension("sum")));
        Assert.assertEquals("3", Iterables.getOnlyElement(row.getDimension("len")));

        //deviation of [7,8,9] is 1/3, stddev is sqrt(1/3), approximately 0.8165
        Assert.assertEquals(0.8165, Double.parseDouble(Iterables.getOnlyElement(row.getDimension("stddev"))), 0.0001);

        // we do not support json-path append function for ORC format (see https://github.com/apache/druid/pull/11722)
        Exception exception = Assert.assertThrows(UnsupportedOperationException.class, () -> {
          row.getDimension("append");
        });
        Assert.assertEquals("Unused", exception.getMessage());
      }
      Assert.assertEquals(1, actualRowCount);
    }
  }

  @Test
  public void testNestedColumn() throws IOException
  {
    final OrcInputFormat inputFormat = new OrcInputFormat(
        new JSONPathSpec(true, ImmutableList.of()),
        null,
        new Configuration()
    );
    final InputEntityReader reader = createReader(
        new TimestampSpec("ts", "millis", null),
        new DimensionsSpec(
            ImmutableList.of(
                new AutoTypeColumnSchema("middle", null),
                new AutoTypeColumnSchema("list", null),
                new AutoTypeColumnSchema("map", null)
            )
        ),
        inputFormat,
        "example/orc-file-11-format.orc"
    );
    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("struct_list_struct_int", "json_value(middle, '$.list[1].int1')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("map_struct_int", "json_value(map, '$.chani.int1')", TestExprMacroTable.INSTANCE)
        )
    );
    TransformingInputEntityReader transformingReader = new TransformingInputEntityReader(
        reader,
        transformSpec.toTransformer()
    );
    try (CloseableIterator<InputRow> iterator = transformingReader.read()) {
      int actualRowCount = 0;

      // Check the first row
      Assert.assertTrue(iterator.hasNext());
      InputRow row = iterator.next();
      actualRowCount++;
      Assert.assertEquals(
          ImmutableMap.of(
              "list",
              ImmutableList.of(
                  ImmutableMap.of("int1", 1, "string1", "bye"),
                  ImmutableMap.of("int1", 2, "string1", "sigh")
              )
          ),
          row.getRaw("middle")
      );
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("int1", 3, "string1", "good"),
              ImmutableMap.of("int1", 4, "string1", "bad")
          ),
          row.getRaw("list")
      );
      Assert.assertEquals(
          ImmutableMap.of(),
          row.getRaw("map")
      );
      Assert.assertEquals(2L, row.getRaw("struct_list_struct_int"));
      Assert.assertEquals(DateTimes.of("2000-03-12T15:00:00.0Z"), row.getTimestamp());

      while (iterator.hasNext()) {
        actualRowCount++;
        row = iterator.next();
      }

      // Check the last row
      Assert.assertEquals(
          ImmutableMap.of(
              "list",
              ImmutableList.of(
                  ImmutableMap.of("int1", 1, "string1", "bye"),
                  ImmutableMap.of("int1", 2, "string1", "sigh")
              )
          ),
          row.getRaw("middle")
      );
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("int1", 100000000, "string1", "cat"),
              ImmutableMap.of("int1", -100000, "string1", "in"),
              ImmutableMap.of("int1", 1234, "string1", "hat")
          ),
          row.getRaw("list")
      );
      Assert.assertEquals(
          ImmutableMap.of(
              "chani", ImmutableMap.of("int1", 5, "string1", "chani"),
              "mauddib", ImmutableMap.of("int1", 1, "string1", "mauddib")
          ),
          row.getRaw("map")
      );
      Assert.assertEquals("2", Iterables.getOnlyElement(row.getDimension("struct_list_struct_int")));
      Assert.assertEquals("5", Iterables.getOnlyElement(row.getDimension("map_struct_int")));

      Assert.assertEquals(7500, actualRowCount);
    }
  }

  @Test
  public void testNestedColumnSchemaless() throws IOException
  {
    final OrcInputFormat inputFormat = new OrcInputFormat(
        new JSONPathSpec(true, ImmutableList.of()),
        null,
        new Configuration()
    );
    final InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("ts", "millis", null),
        DimensionsSpec.builder().useSchemaDiscovery(true).build(),
        ColumnsFilter.all(),
        null
    );
    final FileEntity entity = new FileEntity(new File("example/orc-file-11-format.orc"));

    final InputEntityReader reader = inputFormat.createReader(schema, entity, temporaryFolder.newFolder());

    List<String> dims = ImmutableList.of(
        "boolean1",
        "byte1",
        "short1",
        "int1",
        "long1",
        "float1",
        "double1",
        "bytes1",
        "string1",
        "middle",
        "list",
        "map",
        "decimal1"
    );
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int actualRowCount = 0;

      // Check the first row
      Assert.assertTrue(iterator.hasNext());
      InputRow row = iterator.next();

      Assert.assertEquals(dims, row.getDimensions());
      actualRowCount++;
      Assert.assertEquals(
          ImmutableMap.of(
              "list",
              ImmutableList.of(
                  ImmutableMap.of("int1", 1, "string1", "bye"),
                  ImmutableMap.of("int1", 2, "string1", "sigh")
              )
          ),
          row.getRaw("middle")
      );
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("int1", 3, "string1", "good"),
              ImmutableMap.of("int1", 4, "string1", "bad")
          ),
          row.getRaw("list")
      );
      Assert.assertEquals(
          ImmutableMap.of(),
          row.getRaw("map")
      );
      Assert.assertEquals(DateTimes.of("2000-03-12T15:00:00.0Z"), row.getTimestamp());

      while (iterator.hasNext()) {
        actualRowCount++;
        row = iterator.next();
        Assert.assertEquals(dims, row.getDimensions());
      }

      // Check the last row
      Assert.assertEquals(
          ImmutableMap.of(
              "list",
              ImmutableList.of(
                  ImmutableMap.of("int1", 1, "string1", "bye"),
                  ImmutableMap.of("int1", 2, "string1", "sigh")
              )
          ),
          row.getRaw("middle")
      );
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("int1", 100000000, "string1", "cat"),
              ImmutableMap.of("int1", -100000, "string1", "in"),
              ImmutableMap.of("int1", 1234, "string1", "hat")
          ),
          row.getRaw("list")
      );
      Assert.assertEquals(
          ImmutableMap.of(
              "chani", ImmutableMap.of("int1", 5, "string1", "chani"),
              "mauddib", ImmutableMap.of("int1", 1, "string1", "mauddib")
          ),
          row.getRaw("map")
      );

      Assert.assertEquals(7500, actualRowCount);
    }
  }

  @Test
  public void testListMap() throws IOException
  {
    final InputFormat inputFormat = new OrcInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "a_id0", "$.a['id0']"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "b_raw_str", "$.b"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "b0_id0", "$.b[0]['id0']")
            )
        ),
        null,
        new Configuration()
    );
    final InputEntityReader reader = createReader(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            ImmutableList.of(
                new AutoTypeColumnSchema("a", null),
                new AutoTypeColumnSchema("b", null)
            )
        ),
        inputFormat,
        "example/test_list_map.orc"
    );
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("t_a_id0", "json_value(a, '$.id0')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_b0_id0", "json_value(b, '$[0].id0')", TestExprMacroTable.INSTANCE)
        )
    );
    final InputEntityReader transformingReader = new TransformingInputEntityReader(
        reader,
        transformSpec.toTransformer()
    );
    try (CloseableIterator<InputRow> iterator = transformingReader.read()) {
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      /*
        {
          "timestamp": "2022-01-01T00:00:00",
          "a": {"id0": "str0", "id1": "str1"},
          "b": [{"id0": "str0", "id1": "str1"}, {"id0": "str2", "id1": "str3"}]
        }
     */
      Assert.assertEquals(DateTimes.of("2022-01-01T00:00:00.000Z"), row.getTimestamp());
      Assert.assertEquals("str0", Iterables.getOnlyElement(row.getDimension("a_id0")));
      Assert.assertEquals("str0", Iterables.getOnlyElement(row.getDimension("t_a_id0")));
      Assert.assertEquals("str0", Iterables.getOnlyElement(row.getDimension("b0_id0")));
      Assert.assertEquals("str0", Iterables.getOnlyElement(row.getDimension("t_b0_id0")));
      Assert.assertEquals(ImmutableList.of("{id0=str0, id1=str1}", "{id0=str2, id1=str3}"), row.getDimension("b_raw_str"));
      Assert.assertEquals(ImmutableMap.of("id0", "str0", "id1", "str1"), row.getRaw("a"));
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("id0", "str0", "id1", "str1"),
              ImmutableMap.of("id0", "str2", "id1", "str3")
          ),
          row.getRaw("b")
      );
      Assert.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testNestedArray() throws IOException
  {
    final InputFormat inputFormat = new OrcInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "a_0", "$.a[0]"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "b_0", "$.b[0]"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "c_0_0", "$.c[0][0]"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "d_0_1", "$.d[0][1]")
            )
        ),
        null,
        new Configuration()
    );
    final InputEntityReader reader = createReader(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            ImmutableList.of(
                new AutoTypeColumnSchema("a", null),
                new AutoTypeColumnSchema("b", null),
                new AutoTypeColumnSchema("c", null),
                new AutoTypeColumnSchema("d", null),
                new AutoTypeColumnSchema("t_d_0", null)
            )
        ),
        inputFormat,
        "example/test_nested_array.orc"
    );
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(
            new ExpressionTransform("t_a_0", "json_value(a, '$[0]')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_b_0", "json_value(b, '$[0]')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_c_0_0", "json_value(c, '$[0][0]')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_d_0_1", "json_value(d, '$[0][1]')", TestExprMacroTable.INSTANCE),
            new ExpressionTransform("t_d_0", "json_query(d, '$[0]')", TestExprMacroTable.INSTANCE)
        )
    );
    final InputEntityReader transformingReader = new TransformingInputEntityReader(
        reader,
        transformSpec.toTransformer()
    );
    try (CloseableIterator<InputRow> iterator = transformingReader.read()) {
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      /*
        {
          "timestamp": "2022-01-01T00:00:00",
          "a": ["str1", "str2"],
          "b": [1, 2],
          "c": [["str1", "str2"], ["str3", "str4"]],
          "d": [[1, 2], [3, 4]]
        }
       */
      Assert.assertEquals(DateTimes.of("2022-01-01T00:00:00.000Z"), row.getTimestamp());
      Assert.assertEquals("str1", Iterables.getOnlyElement(row.getDimension("a_0")));
      Assert.assertEquals("str1", Iterables.getOnlyElement(row.getDimension("t_a_0")));
      Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("b_0")));
      Assert.assertEquals("1", Iterables.getOnlyElement(row.getDimension("t_b_0")));
      Assert.assertEquals("str1", Iterables.getOnlyElement(row.getDimension("c_0_0")));
      Assert.assertEquals("str1", Iterables.getOnlyElement(row.getDimension("t_c_0_0")));
      Assert.assertEquals("2", Iterables.getOnlyElement(row.getDimension("d_0_1")));
      Assert.assertEquals("2", Iterables.getOnlyElement(row.getDimension("t_d_0_1")));
      Assert.assertEquals(ImmutableList.of("str1", "str2"), row.getRaw("a"));
      Assert.assertEquals(ImmutableList.of(1, 2), row.getRaw("b"));
      Assert.assertEquals(
          ImmutableList.of(ImmutableList.of("str1", "str2"), ImmutableList.of("str3", "str4")),
          row.getRaw("c")
      );
      Assert.assertEquals(
          ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(3, 4)),
          row.getRaw("d")
      );
      Assert.assertArrayEquals(new Object[]{1L, 2L}, (Object[]) row.getRaw("t_d_0"));
      Assert.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testSimpleNullValues() throws IOException
  {
    final InputFormat inputFormat = new OrcInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of()
        ),
        null,
        new Configuration()
    );
    final InputEntityReader reader = createReader(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            ImmutableList.of(
                new StringDimensionSchema("c1"),
                new StringDimensionSchema("c2")
            )
        ),
        inputFormat,
        "example/test_simple.orc"
    );
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assert.assertTrue(iterator.hasNext());
      InputRow row = iterator.next();

      Assert.assertEquals(DateTimes.of("2022-01-01T00:00:00.000Z"), row.getTimestamp());
      Assert.assertEquals("true", Iterables.getOnlyElement(row.getDimension("c1")));
      Assert.assertEquals("str1", Iterables.getOnlyElement(row.getDimension("c2")));

      row = iterator.next();
      Assert.assertEquals(DateTimes.of("2022-01-02T00:00:00.000Z"), row.getTimestamp());
      Assert.assertEquals(ImmutableList.of(), row.getDimension("c1"));
      Assert.assertEquals(ImmutableList.of(), row.getDimension("c2"));
      Assert.assertFalse(iterator.hasNext());
    }
  }

  private InputEntityReader createReader(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      InputFormat inputFormat,
      String dataFile
  ) throws IOException
  {
    final InputRowSchema schema = new InputRowSchema(timestampSpec, dimensionsSpec, ColumnsFilter.all());
    final FileEntity entity = new FileEntity(new File(dataFile));
    return inputFormat.createReader(schema, entity, temporaryFolder.newFolder());
  }
}
