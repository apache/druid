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

package org.apache.druid.java.util.common.parsers;

import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

/**
 * ParseException can be thrown on both ingestion side and query side.
 *
 * During ingestion, ParseException can be thrown in two places, i.e., {@code InputSourceReader#read()}
 * and {@code IncrementalIndex#addToFacts()}. To easily handle ParseExceptions, consider using
 * {@code FilteringCloseableInputRowIterator} and {@code ParseExceptionHandler} to iterate input rows and
 * to add rows to IncrementalIndex, respectively.
 *
 * When you use {@code InputSourceReader#sample()}, the ParseException will not be thrown, but be stored in
 * {@code InputRowListPlusRawValues}.
 *
 * During query, ParseException can be thrown in SQL planning. It should be never thrown once a query plan is
 * constructed.
 */
public class ParseException extends RuntimeException
{
  /**
   * If true, the row was partially parseable, but some columns could not be parsed
   * (e.g., non-numeric values for a numeric column)
   */
  private final boolean fromPartiallyValidRow;

  /**
   * The timestamp in millis when the parse exception occurred.
   */
  private final long timeOfExceptionMillis;

  /**
   * A string representation of the input data that had a parse exception.
   */
  private final String input;

  public ParseException(@Nullable String input, String formatText, Object... arguments)
  {
    this(input, false, formatText, arguments);
  }

  public ParseException(@Nullable String input, boolean fromPartiallyValidRow, String formatText, Object... arguments)
  {
    this(input, fromPartiallyValidRow, null, formatText, arguments);
  }

  public ParseException(@Nullable String input, Throwable cause, String formatText, Object... arguments)
  {
    this(input, false, cause, formatText, arguments);
  }

  public ParseException(@Nullable String input, boolean fromPartiallyValidRow, Throwable cause, String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments), cause);
    this.input = input;
    this.fromPartiallyValidRow = fromPartiallyValidRow;
    this.timeOfExceptionMillis = System.currentTimeMillis();
  }

  public boolean isFromPartiallyValidRow()
  {
    return fromPartiallyValidRow;
  }

  public long getTimeOfExceptionMillis()
  {
    return timeOfExceptionMillis;
  }

  @Nullable
  public String getInput()
  {
    return input;
  }
}
