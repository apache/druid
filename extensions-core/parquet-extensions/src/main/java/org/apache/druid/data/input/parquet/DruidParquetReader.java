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

package org.apache.druid.data.input.parquet;

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.parquet.simple.ParquetGroupFlattenerMaker;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ObjectFlattener;
import org.apache.druid.java.util.common.parsers.ObjectFlatteners;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class DruidParquetReader extends IntermediateRowParsingReader<Group>
{
  private final InputRowSchema inputRowSchema;
  private final ObjectFlattener<Group> flattener;
  private final byte[] buffer = new byte[InputEntity.DEFAULT_FETCH_BUFFER_SIZE];

  private final ParquetReader<Group> reader;
  private final ParquetMetadata metadata;
  private final Closer closer;

  public DruidParquetReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory,
      JSONPathSpec flattenSpec,
      boolean binaryAsString
  ) throws IOException
  {
    this.inputRowSchema = inputRowSchema;
    this.flattener = ObjectFlatteners.create(flattenSpec, new ParquetGroupFlattenerMaker(binaryAsString));

    closer = Closer.create();
    final InputEntity.CleanableFile file = closer.register(source.fetch(temporaryDirectory, buffer));
    final Path path = new Path(file.file().toURI());

    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      reader = closer.register(ParquetReader.builder(new GroupReadSupport(), path).build());
      metadata = ParquetFileReader.readFooter(new Configuration(), path);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currentClassLoader);
    }
  }

  @Override
  protected CloseableIterator<Group> intermediateRowIterator() throws IOException
  {
    return new CloseableIterator<Group>()
    {
      Group value = null;

      @Override
      public boolean hasNext()
      {
        if (value == null) {
          try {
            value = reader.read();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return value != null;
      }

      @Override
      public Group next()
      {
        if (value == null) {
          throw new NoSuchElementException();
        }
        Group currentValue = value;
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
  protected List<InputRow> parseInputRows(Group intermediateRow) throws IOException, ParseException
  {
    return Collections.singletonList(
        MapInputRowParser.parse(
            inputRowSchema.getTimestampSpec(),
            inputRowSchema.getDimensionsSpec(),
            flattener.flatten(intermediateRow)
        )
    );
  }

  @Override
  protected String toJson(Group intermediateRow) throws IOException
  {
    throw new UnsupportedOperationException();
  }
}
