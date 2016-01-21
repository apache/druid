/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
package io.druid.data.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import io.druid.data.input.impl.TimeAndDimsParseSpec;

import static io.druid.data.input.AvroStreamInputRowParserTest.PARSE_SPEC;
import static io.druid.data.input.AvroStreamInputRowParserTest.SCHEMA_MAPPING;
import static io.druid.data.input.AvroStreamInputRowParserTest.TRAVERSING_PARSE_SPEC;
import static io.druid.data.input.AvroStreamInputRowParserTest.assertInputRowCorrect;
import static io.druid.data.input.AvroStreamInputRowParserTest.buildSomeAvroDatum;

public class AvroHadoopInputRowParserTest
{
  private final ObjectMapper jsonMapper = new ObjectMapper();

  @Test
  public void testParseNotFromPigAvroStorage() throws IOException
  {
    GenericRecord record = buildSomeAvroDatum();
    testParse(PARSE_SPEC, record, false, null);
    testParse(TRAVERSING_PARSE_SPEC, record, false, SCHEMA_MAPPING);
  }

  @Test
  public void testParseFromPiggyBankAvroStorage() throws IOException
  {
    GenericRecord record = buildPiggyBankAvro();
    testParse(PARSE_SPEC, record, false, null);
    testParse(TRAVERSING_PARSE_SPEC, record, false, SCHEMA_MAPPING);
  }

  @Test
  public void testParseFromPigAvroStorage() throws IOException
  {
    GenericRecord record = buildPigAvro();
    testParse(PARSE_SPEC, record, true, null);
    testParse(TRAVERSING_PARSE_SPEC, record, true, SCHEMA_MAPPING);
  }

  private void testParse(TimeAndDimsParseSpec parseSpec, GenericRecord record, boolean fromPigAvroStorage,
      Map<String, String> schemaMappings) throws IOException
  {
    AvroHadoopInputRowParser parser = new AvroHadoopInputRowParser(parseSpec, fromPigAvroStorage, schemaMappings);
    AvroHadoopInputRowParser parser2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(parser),
        AvroHadoopInputRowParser.class
    );
    InputRow inputRow = parser2.parse(record);
    assertInputRowCorrect(parseSpec.getDimensionsSpec().getDimensions(), schemaMappings, inputRow);
  }

  public static GenericRecord buildPigAvro() throws IOException
  {
    return buildPigAvro(buildSomeAvroDatum(), "AvroStorage", "AvroStorage");
  }

  public static GenericRecord buildPiggyBankAvro() throws IOException
  {
    return buildPigAvro(
        buildSomeAvroDatum(),
        "org.apache.pig.piggybank.storage.avro.AvroStorage",
        "org.apache.pig.piggybank.storage.avro.AvroStorage("
            + "'field7','{\"type\":\"map\",\"values\":\"int\"}',"
            + "'field8','{\"type\":\"map\",\"values\":\"string\"}'"
            + "'field14','{\"type\":\"record\",\"name\":\"MySubRecord\",\"fields\":["
            + "{\"name\":\"subInt\",\"type\":\"int\"},"
            + "{\"name\":\"subLong\",\"type\":\"long\"}, "
            + "{\"name\":\"subMap\",\"type\":{\"type\":\"map\",\"values\":\"int\"}}, "
            + "{\"name\":\"subArray\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}')"
    );
  }

  private static GenericRecord buildPigAvro(GenericRecord datum, String inputStorage, String outputStorage)
      throws IOException
  {
    final File tmpDir = Files.createTempDir();
    FileReader<GenericRecord> reader = null;
    PigServer pigServer = null;
    try {
      // 0. write avro object into temp file.
      File someAvroDatumFile = new File(tmpDir, "someAvroDatum.avro");
      DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
          new GenericDatumWriter<GenericRecord>()
      );
      dataFileWriter.create(SomeAvroDatum.getClassSchema(), someAvroDatumFile);
      dataFileWriter.append(datum);
      dataFileWriter.close();
      // 1. read avro files into Pig
      pigServer = new PigServer(ExecType.LOCAL);
      pigServer.registerQuery(
          String.format(
              "A = LOAD '%s' USING %s;",
              someAvroDatumFile,
              inputStorage)
      );
      // 2. write new avro file using AvroStorage
      File outputDir = new File(tmpDir, "output");
      pigServer.store("A", String.valueOf(outputDir), outputStorage);
      // 3. read avro object from AvroStorage
      reader = DataFileReader.openReader(
          new File(outputDir, "part-m-00000.avro"),
          new GenericDatumReader<GenericRecord>()
      );
      return reader.next();
    }
    finally {
      if (pigServer != null) {
        pigServer.shutdown();
      }
      Closeables.close(reader, true);
      FileUtils.deleteDirectory(tmpDir);
    }
  }
}
