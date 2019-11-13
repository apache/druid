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

import org.apache.commons.io.LineIterator;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Abstract {@link InputEntityReader} for text format readers such as CSV or JSON.
 */
public abstract class TextReader implements InputEntityReader, InputEntitySampler
{
  private final InputRowSchema inputRowSchema;

  public TextReader(InputRowSchema inputRowSchema)
  {
    this.inputRowSchema = inputRowSchema;
  }

  public InputRowSchema getInputRowSchema()
  {
    return inputRowSchema;
  }

  @Override
  public CloseableIterator<InputRow> read(InputEntity source, File temporaryDirectory) throws IOException
  {
    return lineIterator(source).flatMap(line -> {
      try {
        // since readLine() returns a list, the below line always iterates over the list,
        // which means it calls Iterator.hasNext() and Iterator.next() at least once per line.
        // This could be unnecessary if the line wouldn't be exploded into multiple rows.
        // If this line turned out to be a performance bottleneck, perhaps readLine() interface might not be a good
        // idea. Subclasses could implement read() with some duplicate codes to avoid unnecessary iteration on
        // a singleton list.
        return CloseableIterators.withEmptyBaggage(readLine(line).iterator());
      }
      catch (IOException e) {
        throw new ParseException(e, "Unable to parse row [%s]", line);
      }
    });
  }

  @Override
  public CloseableIterator<InputRowListPlusJson> sample(InputEntity<?> source, File temporaryDirectory)
      throws IOException
  {
    return lineIterator(source).map(line -> {
      try {
        return sampleLine(line);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private CloseableIterator<String> lineIterator(InputEntity source) throws IOException
  {
    final LineIterator delegate = new LineIterator(
        new InputStreamReader(source.open(), StringUtils.UTF8_STRING)
    );
    final int numHeaderLines = getNumHeaderLinesToSkip();
    for (int i = 0; i < numHeaderLines && delegate.hasNext(); i++) {
      delegate.nextLine(); // skip lines
    }
    if (needsToProcessHeaderLine() && delegate.hasNext()) {
      processHeaderLine(delegate.nextLine());
    }

    return new CloseableIterator<String>()
    {
      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public String next()
      {
        return delegate.nextLine();
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }
    };
  }

  /**
   * Parses the given line into a list of {@link InputRow}s. Note that some file formats can explode a single line of
   * input into multiple inputRows.
   *
   * This method will be called after {@link #getNumHeaderLinesToSkip()} and {@link #processHeaderLine}.
   */
  public abstract List<InputRow> readLine(String line) throws IOException, ParseException;

  /**
   * TODO
   *
   * Should handle {@link ParseException} properly.
   *
   * @param line
   * @return
   * @throws IOException
   */
  public abstract InputRowListPlusJson sampleLine(String line) throws IOException;

  /**
   * Returns the number of header lines to skip.
   * {@link #processHeaderLine} will be called as many times as the returned number.
   */
  public abstract int getNumHeaderLinesToSkip();

  /**
   * Returns true if the file format needs to process a header line.
   * This method will be called after skipping lines as many as {@link #getNumHeaderLinesToSkip()}.
   */
  public abstract boolean needsToProcessHeaderLine();

  /**
   * Processes a header line. This will be called if {@link #needsToProcessHeaderLine()} = true.
   */
  public abstract void processHeaderLine(String line) throws IOException;
}
