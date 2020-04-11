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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AvroOCFReader extends IntermediateRowParsingReader<GenericRecord>
{
  private final InputRowSchema inputRowSchema;
  private final InputEntity source;
  private final File temporaryDirectory;
  private final ObjectFlattener<GenericRecord> recordFlattener;
  private Schema readerSchema;

  AvroOCFReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory,
      Schema readerSchema,
      JSONPathSpec flattenSpec,
      boolean binaryAsString
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.temporaryDirectory = temporaryDirectory;
    this.readerSchema = readerSchema;
    this.recordFlattener = ObjectFlatteners.create(flattenSpec, new AvroFlattenerMaker(false, binaryAsString));
  }

  private static Schema dataFileSchema(File file) throws IOException
  {
    return new DataFileReader<GenericRecord>(file, new GenericDatumReader<>()).getSchema();
  }

  @Override
  protected CloseableIterator<GenericRecord> intermediateRowIterator() throws IOException
  {
    final Closer closer = Closer.create();

    final byte[] buffer = new byte[InputEntity.DEFAULT_FETCH_BUFFER_SIZE];
    final InputEntity.CleanableFile file = closer.register(source.fetch(temporaryDirectory, buffer));
    final Schema writerSchema = dataFileSchema(file.file());
    if (readerSchema == null) {
      readerSchema = writerSchema;
    }
    final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
    final DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file.file(), datumReader);

    return new CloseableIterator<GenericRecord>()
    {
      @Override
      public boolean hasNext()
      {
        return dataFileReader.hasNext();
      }

      @Override
      public GenericRecord next()
      {
        return dataFileReader.next();
      }

      @Override
      public void close() throws IOException
      {
        closer.close();
      }
    };
  }

  @Override
  protected List<InputRow> parseInputRows(GenericRecord intermediateRow) throws ParseException
  {
    return Collections.singletonList(
        MapInputRowParser.parse(
            inputRowSchema.getTimestampSpec(),
            inputRowSchema.getDimensionsSpec(),
            recordFlattener.flatten(intermediateRow)
        )
    );
  }

  @Override
  protected Map<String, Object> toMap(GenericRecord intermediateRow)
  {
    return recordFlattener.toMap(intermediateRow);
  }
}
