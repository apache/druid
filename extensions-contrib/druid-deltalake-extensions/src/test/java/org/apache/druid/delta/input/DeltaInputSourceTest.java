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

package org.apache.druid.delta.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DeltaInputSourceTest
{
  private static final ObjectWriter DEFAULT_JSON_WRITER = new ObjectMapper().writerWithDefaultPrettyPrinter();
  private static final String DELTA_TABLE_PATH = "src/test/resources/people-delta-table";

  @Test
  public void testReadDeltaLakeFilesSample() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DELTA_TABLE_PATH, null);
    Assert.assertNotNull(deltaInputSource);

    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("no_such_col!!", "auto", DateTimes.of("1970")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("city", "state"))),
        ColumnsFilter.all()
    );

    InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);
    Assert.assertNotNull(inputSourceReader);

    List<InputRowListPlusRawValues> inputRowListPlusRawValues = sampleAllRows(inputSourceReader);
    Assert.assertNotNull(inputRowListPlusRawValues);

    Assert.assertEquals(10, inputRowListPlusRawValues.size());

    final String expectedJson = "{\n"
                                + "  \"birthday\" : 1049418130358332,\n"
                                + "  \"country\" : \"Panama\",\n"
                                + "  \"city\" : \"Eastpointe\",\n"
                                + "  \"surname\" : \"Francis\",\n"
                                + "  \"name\" : \"Darren\",\n"
                                + "  \"state\" : \"Minnesota\",\n"
                                + "  \"email\" : \"rating1998@yandex.com\"\n"
                                + "}";
    Assert.assertEquals(
        expectedJson,
        DEFAULT_JSON_WRITER.writeValueAsString(inputRowListPlusRawValues.get(0).getRawValues())
    );
  }

  @Test
  public void testReadDeltaLakeFilesRead() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DELTA_TABLE_PATH, null);
    Assert.assertNotNull(deltaInputSource);

    InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("birthday", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("city", "state"))),
        ColumnsFilter.all()
    );

    InputSourceReader inputSourceReader = deltaInputSource.reader(schema, null, null);
    Assert.assertNotNull(inputSourceReader);

    List<InputRow> rows = readAllRows(inputSourceReader);
    Assert.assertNotNull(rows);

    Assert.assertEquals(10, rows.size());

    final InputRow firstRow = rows.get(0);
    Assert.assertNotNull(firstRow);

    Assert.assertEquals("2003-04-04T01:02:10.000Z", firstRow.getTimestamp().toString());
    Assert.assertEquals("Panama", firstRow.getDimension("country").get(0));
    Assert.assertEquals("Eastpointe", firstRow.getDimension("city").get(0));
    Assert.assertEquals("Francis", firstRow.getDimension("surname").get(0));
    Assert.assertEquals("Darren", firstRow.getDimension("name").get(0));
    Assert.assertEquals("Minnesota", firstRow.getDimension("state").get(0));
    Assert.assertEquals("rating1998@yandex.com", firstRow.getDimension("email").get(0));
  }


  @Test
  public void testReadDeltaLakeNoSplits() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DELTA_TABLE_PATH, null);
    Assert.assertNotNull(deltaInputSource);

    Stream<InputSplit<DeltaSplit>> splits = deltaInputSource.createSplits(null, null);
    Assert.assertNotNull(splits);
    Assert.assertEquals(1, splits.count());
  }

  @Test
  public void testReadDeltaLakeWithSplits() throws IOException
  {
    final DeltaInputSource deltaInputSource = new DeltaInputSource(DELTA_TABLE_PATH, null);
    Assert.assertNotNull(deltaInputSource);

    Stream<InputSplit<DeltaSplit>> splits1 = deltaInputSource.createSplits(null, null);
    List<InputSplit<DeltaSplit>> splitsCollect1 = splits1.collect(Collectors.toList());
    Assert.assertEquals(1, splitsCollect1.size());

    DeltaInputSource deltaInputSourceWithSplit = new DeltaInputSource(DELTA_TABLE_PATH, splitsCollect1.get(0).get());
    Assert.assertNotNull(deltaInputSourceWithSplit);
    Stream<InputSplit<DeltaSplit>> splits2 = deltaInputSourceWithSplit.createSplits(null, null);
    List<InputSplit<DeltaSplit>> splitsCollect2 = splits2.collect(Collectors.toList());
    Assert.assertEquals(1, splitsCollect2.size());

    Assert.assertEquals(splitsCollect1.get(0).get(), splitsCollect2.get(0).get());
  }

  private List<InputRowListPlusRawValues> sampleAllRows(InputSourceReader reader) throws IOException
  {
    List<InputRowListPlusRawValues> rows = new ArrayList<>();
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  private List<InputRow> readAllRows(InputSourceReader reader) throws IOException
  {
    List<InputRow> rows = new ArrayList<>();
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }
}