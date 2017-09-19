/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.parsers;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CSVParser extends AbstractFlatTextFormatParser
{
  private final au.com.bytecode.opencsv.CSVParser parser = new au.com.bytecode.opencsv.CSVParser();

  public CSVParser(
      @Nullable final String listDelimiter,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows
  )
  {
    super(listDelimiter, hasHeaderRow, maxSkipHeaderRows);
  }

  public CSVParser(
      @Nullable final String listDelimiter,
      final Iterable<String> fieldNames,
      final boolean hasHeaderRow,
      final int maxSkipHeaderRows
  )
  {
    this(listDelimiter, hasHeaderRow, maxSkipHeaderRows);

    setFieldNames(fieldNames);
  }

  @Override
  protected List<String> parseLine(String input) throws IOException
  {
    return Arrays.asList(parser.parseLine(input));
  }

  @VisibleForTesting
  CSVParser(@Nullable final String listDelimiter, final String header)
  {
    this(listDelimiter, false, 0);

    setFieldNames(header);
  }
}
