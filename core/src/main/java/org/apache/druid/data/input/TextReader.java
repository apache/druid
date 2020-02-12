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

import com.google.common.base.Strings;
import org.apache.commons.io.LineIterator;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.ParserUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract {@link InputEntityReader} for text format readers such as CSV or JSON.
 */
public abstract class TextReader extends IntermediateRowParsingReader<String>
{
  private final InputRowSchema inputRowSchema;
  private final InputEntity source;

  public TextReader(InputRowSchema inputRowSchema, InputEntity source)
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
  }

  public InputRowSchema getInputRowSchema()
  {
    return inputRowSchema;
  }

  @Override
  public CloseableIterator<String> intermediateRowIterator()
      throws IOException
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
  @Override
  public abstract List<InputRow> parseInputRows(String intermediateRow) throws IOException, ParseException;

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

  public static List<String> findOrCreateColumnNames(List<String> parsedLine)
  {
    final List<String> columns = new ArrayList<>(parsedLine.size());
    for (int i = 0; i < parsedLine.size(); i++) {
      if (Strings.isNullOrEmpty(parsedLine.get(i))) {
        columns.add(ParserUtils.getDefaultColumnName(i));
      } else {
        columns.add(parsedLine.get(i));
      }
    }
    if (columns.isEmpty()) {
      return ParserUtils.generateFieldNames(parsedLine.size());
    } else {
      ParserUtils.validateFields(columns);
      return columns;
    }
  }
}
