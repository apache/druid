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

import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.CloseableIteratorWithParseContext;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
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
    final CloseableIteratorWithParseContext<T> intermediateRowIteratorWithContext = intermediateRowIteratorWithParseContext();

    return new CloseableIterator<InputRow>()
    {
      // since parseInputRows() returns a list, the below line always iterates over the list,
      // which means it calls Iterator.hasNext() and Iterator.next() at least once per row.
      // This could be unnecessary if the row wouldn't be exploded into multiple inputRows.
      // If this line turned out to be a performance bottleneck, perhaps parseInputRows() interface might not be a
      // good idea. Subclasses could implement read() with some duplicate codes to avoid unnecessary iteration on
      // a singleton list.
      Iterator<InputRow> rows = null;
      long currentRecordNumber = 1;

      @Override
      public boolean hasNext()
      {
        if (rows == null || !rows.hasNext()) {
          if (!intermediateRowIteratorWithContext.hasNext()) {
            return false;
          }
          final T row = intermediateRowIteratorWithContext.next();
          final Map<String, Object> context = intermediateRowIteratorWithContext.parseContext();

          try {
            rows = parseInputRows(row).iterator();
            ++currentRecordNumber;
          }
          catch (IOException e) {
            rows = new ExceptionThrowingIterator(
                new ParseException.Builder()
                    .setInput(String.valueOf(row))
                    .setCause(e)
                    .setMessage(buildParseExceptionMessage(
                        "Unable to parse row [%s]",
                        source(),
                        currentRecordNumber,
                        context,
                        row
                    ))
                    .build()
            );
          }
          catch (ParseException e) {
            ParseException.Builder enrichedParseExceptionBuilder = new ParseException.Builder(e);
            enrichedParseExceptionBuilder.setMessage(buildParseExceptionMessage(
                e.getMessage(),
                source(),
                currentRecordNumber,
                context
            ));
            rows = new ExceptionThrowingIterator(enrichedParseExceptionBuilder.build());
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
        intermediateRowIteratorWithContext.close();
      }
    };
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return intermediateRowIteratorWithParseContext().mapWithParseContext((row, context) -> {

      final List<Map<String, Object>> rawColumnsList;
      try {
        rawColumnsList = toMap(row);
      }
      catch (Exception e) {
        return InputRowListPlusRawValues.of(
            null,
            new ParseException.Builder()
                .setInput(String.valueOf(row))
                .setCause(e)
                .setMessage(buildParseExceptionMessage(
                    "Unable to parse row [%s] into JSON",
                    source(),
                    null,
                    context,
                    row
                ))
                .build()
        );
      }

      if (CollectionUtils.isNullOrEmpty(rawColumnsList)) {
        return InputRowListPlusRawValues.of(
            null,
            new ParseException.Builder()
                .setInput(String.valueOf(row))
                .setMessage(buildParseExceptionMessage(
                    "No map object parsed for row [%s]",
                    source(),
                    null,
                    context,
                    row
                ))
                .build()
        );
      }

      List<InputRow> rows;
      try {
        rows = parseInputRows(row);
      }
      catch (ParseException e) {
        ParseException.Builder enrichedParseExceptionBuilder = new ParseException.Builder(e);
        enrichedParseExceptionBuilder.setMessage(buildParseExceptionMessage(
            e.getMessage(),
            source(),
            null,
            context
        ));
        return InputRowListPlusRawValues.ofList(rawColumnsList, enrichedParseExceptionBuilder.build());
      }
      catch (IOException e) {
        ParseException exception = new ParseException.Builder()
            .setInput(String.valueOf(row))
            .setCause(e)
            .setMessage(buildParseExceptionMessage(
                "No map object parsed for row [%s]",
                source(),
                null,
                context,
                row
            ))
            .build();
        return InputRowListPlusRawValues.ofList(rawColumnsList, exception);
      }

      return InputRowListPlusRawValues.ofList(rawColumnsList, rows);
    });
  }

  /**
   * Creates an iterator of intermediate rows. The returned rows will be consumed by {@link #parseInputRows} and
   * {@link #toMap}. Either this or {@link #intermediateRowIteratorWithParseContext()} should be implemented
   */
  protected CloseableIterator<T> intermediateRowIterator() throws IOException
  {
    throw new UnsupportedEncodingException("intermediateRowIterator not implemented");
  }

  /**
   * Same as {@code intermediateRowIterator}, but it also contains the context map such as the line number to generate
   * the {@link ParseException}.
   */
  protected CloseableIteratorWithParseContext<T> intermediateRowIteratorWithParseContext() throws IOException
  {
    return CloseableIteratorWithParseContext.fromCloseableIterator(intermediateRowIterator());
  }

  /**
   * @return InputEntity which the subclass is reading from. Useful in generating informative {@link ParseException}s
   */
  @Nullable
  protected InputEntity source()
  {
    return null;
  }

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
   *
   * This should return a non-empty list with the same size of the list returned by {@link #parseInputRows} or the returned objects will be discarded
   */
  protected abstract List<Map<String, Object>> toMap(T intermediateRow) throws IOException;

  private String buildParseExceptionMessage(
      String formatString,
      @Nullable InputEntity source,
      @Nullable Long recordNumber,
      Map<String, Object> additionalContext,
      Object... baseArgs
  )
  {
    Map<String, Object> temp = Maps.newHashMap(additionalContext);
    if (source != null) {
      temp.put("source", source.getUri());
    }
    if(recordNumber != null) {
      temp.put("recordNumber", recordNumber);
    }
    return StringUtils.nonStrictFormat(formatString, baseArgs, temp);
  }

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
