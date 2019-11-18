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

import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.util.List;

/**
 * {@link InputEntityReader} that parses bytes into some intermediate rows first, and then into {@link InputRow}s.
 * For example, {@link org.apache.druid.data.input.impl.CsvReader} parses bytes into string lines, and then parses
 * those lines into InputRows.
 *
 * @param <T> type of intermediate row. For example, it can be {@link String} for text formats.
 */
public abstract class IntermediateRowParsingReader<T> implements InputEntityReader
{
  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return intermediateRowIterator().flatMap(row -> {
      try {
        // since parseInputRows() returns a list, the below line always iterates over the list,
        // which means it calls Iterator.hasNext() and Iterator.next() at least once per row.
        // This could be unnecessary if the row wouldn't be exploded into multiple inputRows.
        // If this line turned out to be a performance bottleneck, perhaps parseInputRows() interface might not be a
        // good idea. Subclasses could implement read() with some duplicate codes to avoid unnecessary iteration on
        // a singleton list.
        return CloseableIterators.withEmptyBaggage(parseInputRows(row).iterator());
      }
      catch (IOException e) {
        throw new ParseException(e, "Unable to parse row [%s]", row);
      }
    });
  }

  @Override
  public CloseableIterator<InputRowListPlusJson> sample()
      throws IOException
  {
    return intermediateRowIterator().map(row -> {
      final String json;
      try {
        json = toJson(row);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      try {
        return InputRowListPlusJson.of(parseInputRows(row), json);
      }
      catch (ParseException e) {
        return InputRowListPlusJson.of(json, e);
      }
      catch (IOException e) {
        return InputRowListPlusJson.of(json, new ParseException(e, "Unable to parse row [%s]", row));
      }
    });
  }

  /**
   * Creates an iterator of intermediate rows. The returned rows will be consumed by {@link #parseInputRows} and
   * {@link #toJson}.
   */
  protected abstract CloseableIterator<T> intermediateRowIterator()
      throws IOException;

  /**
   * Parses the given intermediate row into a list of {@link InputRow}s.
   */
  protected abstract List<InputRow> parseInputRows(T intermediateRow) throws IOException, ParseException;

  /**
   * Converts the given intermediate row into a JSON string. The returned JSON will be used by FirehoseSampler.
   * Implementations can use any method to convert the given row into JSON string. If the implementation knows how
   * to convert the given row into {@link java.util.Map}, {@link InputEntityReader#DEFAULT_JSON_WRITER} can be
   * used to convert the Map into JSON string.
   */
  protected abstract String toJson(T intermediateRow) throws IOException;
}
