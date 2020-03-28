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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.druid.data.input.impl.DelimitedInputFormat;

import javax.annotation.Nullable;
import java.util.List;

public class DelimitedParser extends AbstractFlatTextFormatParser
{
  private final Splitter splitter;

  public DelimitedParser(
      @Nullable final String delimiter,
      @Nullable final String listDelimiter,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows
  )
  {
    super(listDelimiter, hasHeaderRow, maxSkipHeaderRows);
    final String finalDelimiter = delimiter != null ? delimiter : FlatTextFormat.DELIMITED.getDefaultDelimiter();

    Preconditions.checkState(
        !finalDelimiter.equals(getListDelimiter()),
        "Cannot have same delimiter and list delimiter of [%s]",
        finalDelimiter
    );

    this.splitter = Splitter.on(finalDelimiter);
  }

  public DelimitedParser(
      @Nullable final String delimiter,
      @Nullable final String listDelimiter,
      final Iterable<String> fieldNames,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows
  )
  {
    this(delimiter, listDelimiter, hasHeaderRow, maxSkipHeaderRows);

    setFieldNames(fieldNames);
  }

  @VisibleForTesting
  DelimitedParser(@Nullable final String delimiter, @Nullable final String listDelimiter, final String header)
  {
    this(delimiter, listDelimiter, false, 0);

    setFieldNames(header);
  }

  @Override
  protected List<String> parseLine(String input)
  {
    return splitToList(input);
  }


  private List<String> splitToList(String input)
  {
    return DelimitedInputFormat.splitToList(splitter, input);
  }
}
