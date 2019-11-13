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

import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class InputRowListPlusJson
{
  @Nullable
  private final List<InputRow> inputRows;

  // TODO: remove
  @Nullable
  private final byte[] raw;

  @Nullable
  private final String jsonRaw;

  @Nullable
  private final ParseException parseException;

  //TODO: remove
  public static InputRowListPlusJson of(@Nullable InputRow inputRow, @Nullable byte[] raw)
  {
    return new InputRowListPlusJson(inputRow == null ? null : Collections.singletonList(inputRow), raw, null, null);
  }

  // TODO: rename
  public static InputRowListPlusJson ofJson(@Nullable InputRow inputRow, @Nullable String jsonRaw)
  {
    return of(inputRow == null ? null : Collections.singletonList(inputRow), jsonRaw);
  }

  public static InputRowListPlusJson of(@Nullable List<InputRow> inputRows, @Nullable String jsonRaw)
  {
    return new InputRowListPlusJson(inputRows, null, jsonRaw, null);
  }

  // TODO: remove
  public static InputRowListPlusJson of(@Nullable byte[] raw, @Nullable ParseException parseException)
  {
    return new InputRowListPlusJson(null, raw, null, parseException);
  }

  public static InputRowListPlusJson of(@Nullable String jsonRaw, @Nullable ParseException parseException)
  {
    return new InputRowListPlusJson(null, null, jsonRaw, parseException);
  }

  // TODO: remove byte[]
  private InputRowListPlusJson(@Nullable List<InputRow> inputRows, @Nullable byte[] raw, @Nullable String jsonRaw, @Nullable ParseException parseException)
  {
    this.inputRows = inputRows;
    this.raw = raw;
    this.jsonRaw = jsonRaw;
    this.parseException = parseException;
  }

  @Nullable
  public InputRow getInputRow()
  {
    return Iterables.getOnlyElement(inputRows);
  }

  @Nullable
  public List<InputRow> getInputRows()
  {
    return inputRows;
  }

  // TODO: remove
  /**
   * The raw, unparsed event (as opposed to an {@link InputRow} which is the output of a parser). The interface default
   * for {@link Firehose#nextRowWithRaw()} sets this to null, so this will only be non-null if nextRowWithRaw() is
   * overridden by an implementation, such as in
   * {@link org.apache.druid.data.input.impl.FileIteratingFirehose#nextRowWithRaw()}. Note that returning the raw row
   * does not make sense for some sources (e.g. non-row based types), so clients should be able to handle this field
   * being unset.
   */
  @Nullable
  public byte[] getRaw()
  {
    return raw;
  }

  @Nullable
  public String getJsonRaw()
  {
    return jsonRaw;
  }

  @Nullable
  public ParseException getParseException()
  {
    return parseException;
  }

  public boolean isEmpty()
  {
    return (inputRows == null || inputRows.isEmpty()) && raw == null && jsonRaw == null && parseException == null;
  }
}
