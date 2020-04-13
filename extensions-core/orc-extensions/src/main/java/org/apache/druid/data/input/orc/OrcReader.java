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

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntity.CleanableFile;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class OrcReader extends IntermediateRowParsingReader<OrcStruct>
{
  private final Configuration conf;
  private final InputRowSchema inputRowSchema;
  private final InputEntity source;
  private final File temporaryDirectory;
  private final ObjectFlattener<OrcStruct> orcStructFlattener;

  OrcReader(
      Configuration conf,
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory,
      JSONPathSpec flattenSpec,
      boolean binaryAsString
  )
  {
    this.conf = conf;
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.temporaryDirectory = temporaryDirectory;
    this.orcStructFlattener = ObjectFlatteners.create(flattenSpec, new OrcStructFlattenerMaker(binaryAsString));
  }

  @Override
  protected CloseableIterator<OrcStruct> intermediateRowIterator() throws IOException
  {
    final Closer closer = Closer.create();

    // We fetch here to cache a copy locally. However, this might need to be changed if we want to split an orc file
    // into several InputSplits in the future.
    final byte[] buffer = new byte[InputEntity.DEFAULT_FETCH_BUFFER_SIZE];
    final CleanableFile file = closer.register(source.fetch(temporaryDirectory, buffer));
    final Path path = new Path(file.file().toURI());

    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    final Reader reader;
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      reader = closer.register(OrcFile.createReader(path, OrcFile.readerOptions(conf)));
    }
    finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
    // The below line will get the schmea to read the whole columns.
    // This can be improved by projecting some columns only what users want in the future.
    final TypeDescription schema = reader.getSchema();
    final RecordReader batchReader = reader.rows(reader.options());
    final OrcMapredRecordReader<OrcStruct> recordReader = new OrcMapredRecordReader<>(batchReader, schema);
    closer.register(recordReader::close);
    return new CloseableIterator<OrcStruct>()
    {
      final NullWritable key = recordReader.createKey();
      OrcStruct value = null;

      @Override
      public boolean hasNext()
      {
        if (value == null) {
          try {
            // The returned OrcStruct in next() can be kept in memory for a while.
            // Here, we create a new instance of OrcStruct before calling RecordReader.next(),
            // so that we can avoid to share the same reference to the "value" across rows.
            value = recordReader.createValue();
            if (!recordReader.next(key, value)) {
              value = null;
            }
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return value != null;
      }

      @Override
      public OrcStruct next()
      {
        if (value == null) {
          throw new NoSuchElementException();
        }
        final OrcStruct currentValue = value;
        value = null;
        return currentValue;
      }

      @Override
      public void close() throws IOException
      {
        closer.close();
      }
    };
  }

  @Override
  protected List<InputRow> parseInputRows(OrcStruct intermediateRow) throws ParseException
  {
    return Collections.singletonList(
        MapInputRowParser.parse(
            inputRowSchema.getTimestampSpec(),
            inputRowSchema.getDimensionsSpec(),
            orcStructFlattener.flatten(intermediateRow)
        )
    );
  }

  @Override
  protected Map<String, Object> toMap(OrcStruct intermediateRow)
  {
    return orcStructFlattener.toMap(intermediateRow);
  }
}
