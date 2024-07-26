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

import org.apache.druid.data.input.impl.FastLineIterator;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.CloseableIteratorWithMetadata;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.ParserUtils;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstract {@link InputEntityReader} for text format readers such as CSV or JSON.
 */
public abstract class TextReader<T> extends IntermediateRowParsingReader<T>
{
  private final InputRowSchema inputRowSchema;
  private final InputEntity source;

  protected TextReader(InputRowSchema inputRowSchema, InputEntity source)
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
  }

  public InputRowSchema getInputRowSchema()
  {
    return inputRowSchema;
  }

  @Override
  public CloseableIteratorWithMetadata<T> intermediateRowIteratorWithMetadata() throws IOException
  {
    final CloseableIterator<T> delegate = makeSourceIterator(source.open());
    final int numHeaderLines = getNumHeaderLinesToSkip();
    for (int i = 0; i < numHeaderLines && delegate.hasNext(); i++) {
      delegate.next(); // skip lines
    }
    if (needsToProcessHeaderLine() && delegate.hasNext()) {
      processHeaderLine(delegate.next());
    }

    return new CloseableIteratorWithMetadata<T>()
    {
      private static final String LINE_KEY = "Line";
      private long currentLineNumber = numHeaderLines + (needsToProcessHeaderLine() ? 1 : 0);

      @Override
      public Map<String, Object> currentMetadata()
      {
        return Collections.singletonMap(LINE_KEY, currentLineNumber);
      }

      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public T next()
      {
        currentLineNumber++;
        return delegate.next();
      }

      @Override
      public void close() throws IOException
      {
        delegate.close();
      }
    };
  }

  @Override
  protected InputEntity source()
  {
    return source;
  }

  /**
   * Parses the given line into a list of {@link InputRow}s. Note that some file formats can explode a single line of
   * input into multiple inputRows.
   *
   * This method will be called after {@link #getNumHeaderLinesToSkip()} and {@link #processHeaderLine}.
   */
  @Override
  public abstract List<InputRow> parseInputRows(T intermediateRow) throws IOException, ParseException;

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
  public abstract void processHeaderLine(T line) throws IOException;

  protected abstract CloseableIterator<T> makeSourceIterator(InputStream in);

  public static RowSignature findOrCreateInputRowSignature(List<String> parsedLine)
  {
    final List<String> columns = new ArrayList<>(parsedLine.size());
    for (int i = 0; i < parsedLine.size(); i++) {
      if (com.google.common.base.Strings.isNullOrEmpty(parsedLine.get(i))) {
        columns.add(ParserUtils.getDefaultColumnName(i));
      } else {
        columns.add(parsedLine.get(i));
      }
    }

    ParserUtils.validateFields(columns);
    final RowSignature.Builder builder = RowSignature.builder();
    for (final String column : columns) {
      builder.add(column, null);
    }
    return builder.build();
  }

  public abstract static class Strings extends TextReader<String>
  {
    protected Strings(InputRowSchema inputRowSchema, InputEntity source)
    {
      super(inputRowSchema, source);
    }

    @Override
    protected CloseableIterator<String> makeSourceIterator(InputStream in)
    {
      return new FastLineIterator.Strings(in);
    }
  }

  public abstract static class Bytes extends TextReader<byte[]>
  {
    protected Bytes(InputRowSchema inputRowSchema, InputEntity source)
    {
      super(inputRowSchema, source);
    }

    @Override
    protected CloseableIterator<byte[]> makeSourceIterator(InputStream in)
    {
      return new FastLineIterator.Bytes(in);
    }

    @Override
    protected String intermediateRowAsString(@Nullable byte[] row)
    {
      // Like String.valueOf, but for UTF-8 bytes. Keeps error messages consistent between String and Bytes.
      return row == null ? "null" : StringUtils.fromUtf8(row);
    }
  }
}
