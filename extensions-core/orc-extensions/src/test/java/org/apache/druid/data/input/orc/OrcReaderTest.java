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
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FileEntity;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class OrcReaderTest
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
        new DimensionsSpec(null, Collections.singletonList("time"), null),
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
        new DimensionsSpec(null, Collections.singletonList("time"), null),
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

  private InputEntityReader createReader(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      InputFormat inputFormat,
      String dataFile
  ) throws IOException
  {
    final InputRowSchema schema = new InputRowSchema(timestampSpec, dimensionsSpec, Collections.emptyList());
    final FileEntity entity = new FileEntity(new File(dataFile));
    return inputFormat.createReader(schema, entity, temporaryFolder.newFolder());
  }
}
