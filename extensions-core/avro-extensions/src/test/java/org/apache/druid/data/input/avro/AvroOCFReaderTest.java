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

package org.apache.druid.data.input.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.avro.generic.GenericRecord;
import org.apache.druid.data.input.AvroHadoopInputRowParserTest;
import org.apache.druid.data.input.AvroStreamInputRowParserTest;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FileEntity;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AvroOCFReaderTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testParse() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, mapper)
    );
    final InputEntityReader reader = createReader(mapper, null);
    assertRow(reader);
  }

  @Test
  public void testParseWithReaderSchema() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, mapper)
    );

    // Read the data using a reduced reader schema, emulate using an older version with less fields
    String schemaStr = "{\n"
                       + "  \"namespace\": \"org.apache.druid.data.input\",\n"
                       + "  \"name\": \"SomeAvroDatum\",\n"
                       + "  \"type\": \"record\",\n"
                       + "  \"fields\" : [\n"
                       + "    {\"name\":\"timestamp\",\"type\":\"long\"},\n"
                       + "    {\"name\":\"eventType\",\"type\":\"string\"},\n"
                       + "    {\"name\":\"someLong\",\"type\":\"long\"}\n"
                       + "  ]\n"
                       + "}";

    TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>()
    {
    };
    final Map<String, Object> readerSchema = mapper.readValue(schemaStr, typeRef);

    final InputEntityReader reader = createReader(mapper, readerSchema);

    assertRow(reader);
  }

  @Test
  public void testParseWithReaderSchemaAlias() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, mapper)
    );

    // Read the data using a reduced reader schema, emulate using an older version with less fields
    String schemaStr = "{\n"
                       + "  \"namespace\": \"org.apache.druid.data.input\",\n"
                       + "  \"name\": \"SomeAvroDatum\",\n"
                       + "  \"type\": \"record\",\n"
                       + "  \"fields\" : [\n"
                       + "    {\"name\":\"timestamp\",\"type\":\"long\"},\n"
                       + "    {\"name\":\"someLong\",\"type\":\"long\"}\n,"
                       + "    {\"name\":\"eventClass\",\"type\":\"string\", \"aliases\": [\"eventType\"]}\n"
                       + "  ]\n"
                       + "}";

    TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>()
    {
    };
    final Map<String, Object> readerSchema = mapper.readValue(schemaStr, typeRef);

    final InputEntityReader reader = createReader(mapper, readerSchema);

    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      // eventType is aliased to eventClass in the reader schema and should be transformed at read time
      Assert.assertEquals("type-a", Iterables.getOnlyElement(row.getDimension("eventClass")));
      Assert.assertFalse(iterator.hasNext());
    }
  }

  private void assertRow(InputEntityReader reader) throws IOException
  {
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assert.assertTrue(iterator.hasNext());
      final InputRow row = iterator.next();
      Assert.assertEquals(DateTimes.of("2015-10-25T19:30:00.000Z"), row.getTimestamp());
      Assert.assertEquals("type-a", Iterables.getOnlyElement(row.getDimension("eventType")));
      Assert.assertEquals(679865987569912369L, row.getMetric("someLong"));
      Assert.assertFalse(iterator.hasNext());
    }
  }

  private InputEntityReader createReader(
      ObjectMapper mapper,
      Map<String, Object> readerSchema
  ) throws Exception
  {
    final GenericRecord someAvroDatum = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    final File someAvroFile = AvroHadoopInputRowParserTest.createAvroFile(someAvroDatum);
    final TimestampSpec timestampSpec = new TimestampSpec("timestamp", "auto", null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of(
        "eventType")));
    final List<String> metricNames = ImmutableList.of("someLong");

    final AvroOCFInputFormat inputFormat = new AvroOCFInputFormat(mapper, null, readerSchema, null);
    final InputRowSchema schema = new InputRowSchema(timestampSpec, dimensionsSpec, metricNames);
    final FileEntity entity = new FileEntity(someAvroFile);
    return inputFormat.createReader(schema, entity, temporaryFolder.newFolder());
  }
}
