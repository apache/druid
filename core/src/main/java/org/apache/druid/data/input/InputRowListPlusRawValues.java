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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A triple of a list of {@link InputRow}s, a {@link Map} of raw values, and a {@link ParseException}.
 * The rawValues map contains the raw values before being parsed into InputRows. Note that a single map can be parsed
 * into multiple InputRows, for example, with explodeSpec.
 * The ParseException is the exception thrown when parsing bytes into either the rawValues map or the list of InputRows.
 *
 * In any case, one of triple must not be null.
 */
public class InputRowListPlusRawValues
{
  @Nullable
  private final List<InputRow> inputRows;

  @Nullable
  private final Map<String, Object> rawValues;

  @Nullable
  private final ParseException parseException;

  public static InputRowListPlusRawValues of(@Nullable InputRow inputRow, Map<String, Object> rawColumns)
  {
    return of(inputRow == null ? null : Collections.singletonList(inputRow), rawColumns);
  }

  public static InputRowListPlusRawValues of(@Nullable List<InputRow> inputRows, Map<String, Object> rawColumns)
  {
    return new InputRowListPlusRawValues(inputRows, Preconditions.checkNotNull(rawColumns, "rawColumns"), null);
  }

  public static InputRowListPlusRawValues of(@Nullable Map<String, Object> rawColumns, ParseException parseException)
  {
    return new InputRowListPlusRawValues(
        null,
        rawColumns,
        Preconditions.checkNotNull(parseException, "parseException")
    );
  }

  private InputRowListPlusRawValues(
      @Nullable List<InputRow> inputRows,
      @Nullable Map<String, Object> rawValues,
      @Nullable ParseException parseException
  )
  {
    this.inputRows = inputRows;
    this.rawValues = rawValues;
    this.parseException = parseException;
  }

  @Nullable
  public List<InputRow> getInputRows()
  {
    return inputRows;
  }

  @Nullable
  public Map<String, Object> getRawValues()
  {
    return rawValues;
  }

  @Nullable
  public ParseException getParseException()
  {
    return parseException;
  }
}
