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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.CloseableIteratorWithMetadata;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    final CloseableIteratorWithMetadata<T> intermediateRowIteratorWithMetadata = intermediateRowIteratorWithMetadata();

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
          if (!intermediateRowIteratorWithMetadata.hasNext()) {
            return false;
          }
          final T row = intermediateRowIteratorWithMetadata.next();
          try {
            rows = parseInputRows(row).iterator();
            ++currentRecordNumber;
          }
          catch (IOException e) {
            final Map<String, Object> metadata = intermediateRowIteratorWithMetadata.currentMetadata();
            final String rowAsString = intermediateRowAsString(row);
            rows = new ExceptionThrowingIterator(new ParseException(
                rowAsString,
                e,
                buildParseExceptionMessage(
                    StringUtils.format("Unable to parse row [%s]", rowAsString),
                    source(),
                    currentRecordNumber,
                    metadata
                )
            ));
          }
          catch (ParseException e) {
            final Map<String, Object> metadata = intermediateRowIteratorWithMetadata.currentMetadata();
            // Replace the message of the ParseException e
            rows = new ExceptionThrowingIterator(
                new ParseException(
                    e.getInput(),
                    e.isFromPartiallyValidRow(),
                    buildParseExceptionMessage(e.getMessage(), source(), currentRecordNumber, metadata)
                ));
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
        intermediateRowIteratorWithMetadata.close();
      }
    };
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {

    final CloseableIteratorWithMetadata<T> delegate = intermediateRowIteratorWithMetadata();

    return new CloseableIterator<InputRowListPlusRawValues>()
    {
      @Override
      public void close() throws IOException
      {
        delegate.close();
      }

      @Override
      public boolean hasNext()
      {
        return delegate.hasNext();
      }

      @Override
      public InputRowListPlusRawValues next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        return sampleIntermediateRow(delegate.next(), delegate.currentMetadata());
      }
    };
  }

  /**
   * Parses and samples the intermediate row and returns input row and the raw values in it. Metadata supplied can
   * contain information about the source which will get surfaced in case an exception occurs while parsing the
   * intermediate row
   *
   * @param row intermediate row
   * @param metadata additional information about the source and the record getting parsed
   * @return sampled data from the intermediate row
   */
  private InputRowListPlusRawValues sampleIntermediateRow(T row, Map<String, Object> metadata)
  {

    final List<Map<String, Object>> rawColumnsList;
    try {
      rawColumnsList = toMap(row);
    }
    catch (Exception e) {
      final String rowAsString = intermediateRowAsString(row);
      return InputRowListPlusRawValues.of(
          null,
          new ParseException(rowAsString, e, buildParseExceptionMessage(
              StringUtils.nonStrictFormat("Unable to parse row [%s] into JSON", rowAsString),
              source(),
              null,
              metadata
          ))
      );
    }

    if (CollectionUtils.isNullOrEmpty(rawColumnsList)) {
      final String rowAsString = intermediateRowAsString(row);
      return InputRowListPlusRawValues.of(
          null,
          new ParseException(rowAsString, buildParseExceptionMessage(
              StringUtils.nonStrictFormat("No map object parsed for row [%s]", rowAsString),
              source(),
              null,
              metadata
          ))
      );
    }

    List<InputRow> rows;
    try {
      rows = parseInputRows(row);
    }
    catch (ParseException e) {
      return InputRowListPlusRawValues.ofList(rawColumnsList, new ParseException(
          intermediateRowAsString(row),
          e,
          buildParseExceptionMessage(e.getMessage(), source(), null, metadata)
      ));
    }
    catch (IOException e) {
      final String rowAsString = intermediateRowAsString(row);
      ParseException exception = new ParseException(rowAsString, e, buildParseExceptionMessage(
          StringUtils.nonStrictFormat("Unable to parse row [%s] into inputRow", rowAsString),
          source(),
          null,
          metadata
      ));
      return InputRowListPlusRawValues.ofList(rawColumnsList, exception);
    }

    return InputRowListPlusRawValues.ofList(rawColumnsList, rows);
  }

  /**
   * Creates an iterator of intermediate rows. The returned rows will be consumed by {@link #parseInputRows} and
   * {@link #toMap}. Either this or {@link #intermediateRowIteratorWithMetadata()} should be implemented
   */
  protected CloseableIterator<T> intermediateRowIterator() throws IOException
  {
    throw new UOE("intermediateRowIterator not implemented");
  }

  /**
   * Same as {@code intermediateRowIterator}, but it also contains the metadata such as the line number to generate
   * more informative {@link ParseException}.
   */
  protected CloseableIteratorWithMetadata<T> intermediateRowIteratorWithMetadata() throws IOException
  {
    return CloseableIteratorWithMetadata.withEmptyMetadata(intermediateRowIterator());
  }

  /**
   * String representation of an intermediate row. Used for error messages.
   */
  protected String intermediateRowAsString(@Nullable T row)
  {
    return String.valueOf(row);
  }

  /**
   * @return InputEntity which the implementation is reading from. Useful in generating informative {@link ParseException}s.
   * For example, in case of {@link org.apache.druid.data.input.impl.FileEntity}, file name containing erroneous records
   * or in case of {@link org.apache.druid.data.input.impl.HttpEntity}, the endpoint containing the erroneous data can
   * be attached to the error message
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

  /**
   * A helper method which enriches the base parse exception message with additional information. The returned message
   * has a format: "baseExceptionMessage (key1: value1, key2: value2)" if additional properties are present. Else it
   * returns the baseException message without any modification
   */
  private static String buildParseExceptionMessage(
      @Nonnull String baseExceptionMessage,
      @Nullable InputEntity source,
      @Nullable Long recordNumber,
      @Nullable Map<String, Object> metadata
  )
  {
    StringBuilder sb = new StringBuilder();
    if (source != null && source.getUri() != null) {
      sb.append(StringUtils.format("Path: %s, ", source.getUri()));
    }
    if (recordNumber != null) {
      sb.append(StringUtils.format("Record: %d, ", recordNumber));
    }
    if (metadata != null) {
      metadata.entrySet().stream()
              .map(entry -> StringUtils.format("%s: %s, ", entry.getKey(), entry.getValue().toString()))
              .forEach(sb::append);
    }
    if (sb.length() == 0) {
      return baseExceptionMessage;
    }
    sb.setLength(sb.length() - 2); // Erase the last stray ", "
    return baseExceptionMessage + " (" + sb + ")"; // Wrap extra information in a bracket before returning
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
