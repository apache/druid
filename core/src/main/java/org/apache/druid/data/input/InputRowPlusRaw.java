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

import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;

public class InputRowPlusRaw
{
  @Nullable
  private final InputRow inputRow;

  @Nullable
  private final byte[] raw;

  @Nullable
  private final ParseException parseException;

  private InputRowPlusRaw(@Nullable InputRow inputRow, @Nullable byte[] raw, @Nullable ParseException parseException)
  {
    this.inputRow = inputRow;
    this.raw = raw;
    this.parseException = parseException;
  }

  @Nullable
  public InputRow getInputRow()
  {
    return inputRow;
  }

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
  public ParseException getParseException()
  {
    return parseException;
  }

  public boolean isEmpty()
  {
    return inputRow == null && raw == null && parseException == null;
  }

  public static InputRowPlusRaw of(@Nullable InputRow inputRow, @Nullable byte[] raw)
  {
    return new InputRowPlusRaw(inputRow, raw, null);
  }

  public static InputRowPlusRaw of(@Nullable byte[] raw, @Nullable ParseException parseException)
  {
    return new InputRowPlusRaw(null, raw, parseException);
  }
}
