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

public class InputRowListPlusJson
{
  @Nullable
  private final List<InputRow> inputRows;

  @Nullable
  private final Map<String, Object> rawColumns;

  @Nullable
  private final ParseException parseException;

  public static InputRowListPlusJson of(@Nullable InputRow inputRow, Map<String, Object> rawColumns)
  {
    return of(inputRow == null ? null : Collections.singletonList(inputRow), rawColumns);
  }

  public static InputRowListPlusJson of(@Nullable List<InputRow> inputRows, Map<String, Object> rawColumns)
  {
    return new InputRowListPlusJson(inputRows, Preconditions.checkNotNull(rawColumns, "rawColumns"), null);
  }

  public static InputRowListPlusJson of(@Nullable Map<String, Object> rawColumns, ParseException parseException)
  {
    return new InputRowListPlusJson(null, rawColumns, Preconditions.checkNotNull(parseException, "parseException"));
  }

  private InputRowListPlusJson(
      @Nullable List<InputRow> inputRows,
      @Nullable Map<String, Object> rawColumns,
      @Nullable ParseException parseException
  )
  {
    this.inputRows = inputRows;
    this.rawColumns = rawColumns;
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
    return rawColumns;
  }

  @Nullable
  public ParseException getParseException()
  {
    return parseException;
  }
}
