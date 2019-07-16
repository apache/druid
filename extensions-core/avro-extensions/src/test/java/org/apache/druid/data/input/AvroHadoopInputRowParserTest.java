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

package org.apache.druid.data.input;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.druid.data.input.avro.AvroExtensionsModule;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;


public class AvroHadoopInputRowParserTest
{
  private final ObjectMapper jsonMapper = new ObjectMapper();

  @Before
  public void setUp()
  {
    for (Module jacksonModule : new AvroExtensionsModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
  }

  @Test
  public void testParseNotFromSpark() throws IOException
  {
    testParse(AvroStreamInputRowParserTest.buildSomeAvroDatum());
  }

  @Test
  public void testParseFromSpark() throws IOException
  {
    testParse(buildAvroFromFile());
  }

  private void testParse(GenericRecord record) throws IOException
  {
    AvroHadoopInputRowParser parser = new AvroHadoopInputRowParser(AvroStreamInputRowParserTest.PARSE_SPEC);
    AvroHadoopInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        AvroHadoopInputRowParser.class
    );
    InputRow inputRow = parser2.parseBatch(record).get(0);
    AvroStreamInputRowParserTest.assertInputRowCorrect(inputRow, AvroStreamInputRowParserTest.DIMENSIONS);
  }

  private static GenericRecord buildAvroFromFile() throws IOException
  {
    return buildAvroFromFile(
        AvroStreamInputRowParserTest.buildSomeAvroDatum()
    );
  }

  private static GenericRecord buildAvroFromFile(GenericRecord datum)
      throws IOException
  {
    final File tmpDir = Files.createTempDir();

    // 0. write avro object into temp file.
    File someAvroDatumFile = new File(tmpDir, "someAvroDatum.avro");
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(
        new SpecificDatumWriter<>()
    )) {
      dataFileWriter.create(SomeAvroDatum.getClassSchema(), someAvroDatumFile);
      dataFileWriter.append(datum);
    }

    final GenericRecord record;
    // 3. read avro object from AvroStorage
    try (FileReader<GenericRecord> reader = DataFileReader.openReader(
        someAvroDatumFile,
        new GenericDatumReader<>()
    )) {
      record = reader.next();
    }

    return record;
  }
}
