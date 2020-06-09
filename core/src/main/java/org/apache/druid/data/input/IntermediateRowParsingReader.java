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

import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * {@link InputEntityReader} that parses bytes into some intermediate rows first, and then into {@link InputRow}s.
 * For example, {@link org.apache.druid.data.input.impl.DelimitedValueReader} parses bytes into string lines, and then parses
 * those lines into InputRows.
 *
 * @param <T> type of intermediate row. For example, it can be {@link String} for text formats.
 */
public abstract class IntermediateRowParsingReader<T> implements InputEntityReader
{
  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    final CloseableIterator<T> intermediateRowIterator = intermediateRowIterator();

    return new CloseableIterator<InputRow>()
    {
      // since parseInputRows() returns a list, the below line always iterates over the list,
      // which means it calls Iterator.hasNext() and Iterator.next() at least once per row.
      // This could be unnecessary if the row wouldn't be exploded into multiple inputRows.
      // If this line turned out to be a performance bottleneck, perhaps parseInputRows() interface might not be a
      // good idea. Subclasses could implement read() with some duplicate codes to avoid unnecessary iteration on
      // a singleton list.
      Iterator<InputRow> rows = null;

      @Override
      public boolean hasNext()
      {
        if (rows == null || !rows.hasNext()) {
          if (!intermediateRowIterator.hasNext()) {
            return false;
          }
          final T row = intermediateRowIterator.next();
          try {
            rows = parseInputRows(row).iterator();
          }
          catch (IOException e) {
            rows = new ExceptionThrowingIterator(new ParseException(e, "Unable to parse row [%s]", row));
          }
          catch (ParseException e) {
            rows = new ExceptionThrowingIterator(e);
          }
        }

        return true;
      }

      @Override
      public InputRow next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        return rows.next();
      }

      @Override
      public void close() throws IOException
      {
        intermediateRowIterator.close();
      }
    };
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return intermediateRowIterator().map(row -> {
      final Map<String, Object> rawColumns;
      try {
        rawColumns = toMap(row);
      }
      catch (Exception e) {
        return InputRowListPlusRawValues.of(null, new ParseException(e, "Unable to parse row [%s] into JSON", row));
      }
      try {
        return InputRowListPlusRawValues.of(parseInputRows(row), rawColumns);
      }
      catch (ParseException e) {
        return InputRowListPlusRawValues.of(rawColumns, e);
      }
      catch (IOException e) {
        return InputRowListPlusRawValues.of(rawColumns, new ParseException(e, "Unable to parse row [%s] into inputRow", row));
      }
    });
  }

  /**
   * Creates an iterator of intermediate rows. The returned rows will be consumed by {@link #parseInputRows} and
   * {@link #toMap}.
   */
  protected abstract CloseableIterator<T> intermediateRowIterator() throws IOException;

  /**
   * Parses the given intermediate row into a list of {@link InputRow}s.
   * This should return a non-empty list.
   *
   * @throws ParseException if it cannot parse the given intermediateRow properly
   */
  protected abstract List<InputRow> parseInputRows(T intermediateRow) throws IOException, ParseException;

  /**
   * Converts the given intermediate row into a {@link Map}. The returned JSON will be used by InputSourceSampler.
   * Implementations can use any method to convert the given row into a Map.
   */
  protected abstract Map<String, Object> toMap(T intermediateRow) throws IOException;

  private static class ExceptionThrowingIterator implements CloseableIterator<InputRow>
  {
    private final Exception exception;

    private boolean thrown = false;

    private ExceptionThrowingIterator(Exception exception)
    {
      this.exception = exception;
    }

    @Override
    public boolean hasNext()
    {
      return !thrown;
    }

    @Override
    public InputRow next()
    {
      thrown = true;
      if (exception instanceof RuntimeException) {
        throw (RuntimeException) exception;
      } else {
        throw new RuntimeException(exception);
      }
    }

    @Override
    public void close()
    {
      // do nothing
    }
  }
}
