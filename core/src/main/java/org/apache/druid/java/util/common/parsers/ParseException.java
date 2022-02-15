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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * ParseException can be thrown on both ingestion side and query side.
 * <p>
 * During ingestion, ParseException can be thrown in two places, i.e., {@code InputSourceReader#read()}
 * and {@code IncrementalIndex#addToFacts()}. To easily handle ParseExceptions, consider using
 * {@code FilteringCloseableInputRowIterator} and {@code ParseExceptionHandler} to iterate input rows and
 * to add rows to IncrementalIndex, respectively.
 * <p>
 * When you use {@code InputSourceReader#sample()}, the ParseException will not be thrown, but be stored in
 * {@code InputRowListPlusRawValues}.
 * <p>
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
    super(StringUtils.nonStrictFormat(formatText, arguments));
    this.input = input;
    this.fromPartiallyValidRow = false;
    this.timeOfExceptionMillis = System.currentTimeMillis();
  }

  public ParseException(@Nullable String input, boolean fromPartiallyValidRow, String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments));
    this.input = input;
    this.fromPartiallyValidRow = fromPartiallyValidRow;
    this.timeOfExceptionMillis = System.currentTimeMillis();
  }

  public ParseException(@Nullable String input, Throwable cause, String formatText, Object... arguments)
  {
    this(input, false, StringUtils.nonStrictFormat(formatText, arguments), cause);
  }

  /**
   * To be called from the Builder
   */
  private ParseException(
      @Nullable String input,
      @Nullable Throwable cause,
      @Nonnull String message,
      boolean fromPartiallyValidRow
  )
  {
    super(StringUtils.nonStrictFormat(message, cause));
    this.timeOfExceptionMillis = System.currentTimeMillis();
    this.fromPartiallyValidRow = fromPartiallyValidRow;
    this.input = input;
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

  /**
   * Builder for {@link ParseException}
   */
  public static class Builder
  {

    String input = null;
    String message = null;
    Throwable cause = null;
    boolean fromPartiallyValidRow = false;

    public Builder()
    {
    }

    public Builder(ParseException partialException)
    {
      this.input = partialException.getInput();
      this.message = partialException.getMessage();
      this.cause = partialException.getCause();
      this.fromPartiallyValidRow = partialException.isFromPartiallyValidRow();
    }

    public Builder setInput(String input)
    {
      this.input = input;
      return this;
    }

    public Builder setMessage(String formatString, Object... arguments)
    {
      this.message = StringUtils.nonStrictFormat(formatString, arguments);
      return this;
    }

    public Builder setCause(Throwable cause)
    {
      this.cause = cause;
      return this;
    }

    public Builder setFromPartiallyValidRow(boolean fromPartiallyValidRow)
    {
      this.fromPartiallyValidRow = fromPartiallyValidRow;
      return this;
    }

    public ParseException build()
    {
      Preconditions.checkNotNull(message, "message not supplied to the builder");
      return new ParseException(input, cause, message, fromPartiallyValidRow);
    }
  }
}
