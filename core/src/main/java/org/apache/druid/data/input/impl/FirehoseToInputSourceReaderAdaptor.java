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

package org.apache.druid.data.input.impl;

import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

public class FirehoseToInputSourceReaderAdaptor implements InputSourceReader
{
  private final FirehoseFactory firehoseFactory;
  private final InputRowParser inputRowParser;
  private final File temporaryDirectory;

  public FirehoseToInputSourceReaderAdaptor(
      FirehoseFactory firehoseFactory,
      InputRowParser inputRowParser,
      File temporaryDirectory
  )
  {
    this.firehoseFactory = firehoseFactory;
    this.inputRowParser = inputRowParser;
    this.temporaryDirectory = temporaryDirectory;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return new CloseableIterator<InputRow>()
    {
      final Firehose firehose = firehoseFactory.connect(inputRowParser, temporaryDirectory);

      @Override
      public boolean hasNext()
      {
        try {
          return firehose.hasMore();
        }
        catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public InputRow next()
      {
        try {
          return firehose.nextRow();
        }
        catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public void close() throws IOException
      {
        firehose.close();
      }
    };
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return new CloseableIterator<InputRowListPlusRawValues>()
    {
      final Firehose firehose = firehoseFactory.connectForSampler(inputRowParser, temporaryDirectory);

      @Override
      public boolean hasNext()
      {
        try {
          return firehose.hasMore();
        }
        catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public InputRowListPlusRawValues next()
      {
        try {
          return firehose.nextRowWithRaw();
        }
        catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      @Override
      public void close() throws IOException
      {
        firehose.close();
      }
    };
  }
}
