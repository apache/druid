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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowPlusRaw;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.SplitReader;
import org.apache.druid.data.input.SplitSource;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * InputSourceReader iterating multiple {@link SplitSource}s.
 */
public class SplitIteratingReader<T> implements InputSourceReader
{
  private final TimestampSpec timestampSpec;
  private final DimensionsSpec dimensionsSpec;
  private final InputFormat inputFormat;
  private final Iterator<SplitSource<T>> sourceIterator;
  private final File temporaryDirectory;

  public SplitIteratingReader(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      InputFormat inputFormat,
      Stream<SplitSource<T>> sourceStream,
      File temporaryDirectory
  )
  {
    this.timestampSpec = timestampSpec;
    this.dimensionsSpec = dimensionsSpec;
    this.inputFormat = inputFormat;
    this.sourceIterator = sourceStream.iterator();
    this.temporaryDirectory = temporaryDirectory;
  }

  @Override
  public CloseableIterator<InputRow> read()
  {
    return createIterator(reader -> {
      try {
        return reader.read(sourceIterator.next(), temporaryDirectory);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public CloseableIterator<InputRowPlusRaw> sample()
  {
    return createIterator(reader -> {
      try {
        return reader.sample(sourceIterator.next(), temporaryDirectory);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private <R> CloseableIterator<R> createIterator(Function<SplitReader, CloseableIterator<R>> rowPopulator)
  {
    return new CloseableIterator<R>()
    {
      CloseableIterator<R> rowIterator = null;

      @Override
      public boolean hasNext()
      {
        checkRowIterator();
        return rowIterator != null && rowIterator.hasNext();
      }

      @Override
      public R next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return rowIterator.next();
      }

      private void checkRowIterator()
      {
        if (rowIterator == null || !rowIterator.hasNext()) {
          try {
            if (rowIterator != null) {
              rowIterator.close();
            }
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
          if (sourceIterator.hasNext()) {
            // SplitSampler is stateful and so a new one should be created per split.
            final SplitReader splitReader = inputFormat.createReader(timestampSpec, dimensionsSpec);
            rowIterator = rowPopulator.apply(splitReader);
          }
        }
      }

      @Override
      public void close() throws IOException
      {
        if (rowIterator != null) {
          rowIterator.close();
        }
      }
    };
  }
}
