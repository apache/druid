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

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class DelimitedParser extends AbstractFlatTextFormatParser
{
  private final String delimiter;
  private final Splitter splitter;

  public DelimitedParser(
      @Nullable final String delimiter,
      @Nullable final String listDelimiter,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows
  )
  {
    super(listDelimiter, hasHeaderRow, maxSkipHeaderRows);
    this.delimiter = delimiter != null ? delimiter : FlatTextFormat.DELIMITED.getDefaultDelimiter();

    Preconditions.checkState(
        !this.delimiter.equals(getListDelimiter()),
        "Cannot have same delimiter and list delimiter of [%s]",
        this.delimiter
    );

    this.splitter = Splitter.on(this.delimiter);
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

  /**
   * Copied from Guava's {@link Splitter#splitToList(CharSequence)}.
   * This is to avoid the error of the missing method signature when using an old Guava library.
   * For example, it may happen when running Druid Hadoop indexing jobs, since we may inherit the version provided by
   * the Hadoop cluster. See https://github.com/apache/incubator-druid/issues/6801.
   */
  private List<String> splitToList(String input)
  {
    Preconditions.checkNotNull(input);

    Iterator<String> iterator = splitter.split(input).iterator();
    List<String> result = new ArrayList<String>();

    while (iterator.hasNext()) {
      result.add(iterator.next());
    }

    return Collections.unmodifiableList(result);
  }
}
