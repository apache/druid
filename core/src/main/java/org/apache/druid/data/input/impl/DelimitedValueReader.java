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

package org.apache.druid.data.input.impl;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.TextReader;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.common.parsers.ParserUtils;
import org.apache.druid.java.util.common.parsers.Parsers;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * DelimitedValueReader is the reader for Delimitor Separate Value format input data(CSV/TSV).
 */
public class DelimitedValueReader extends TextReader
{
  private final boolean findColumnsFromHeader;
  private final int skipHeaderRows;
  private final Function<String, Object> multiValueFunction;
  private final DelimitedValueParser parser;
  @Nullable
  private List<String> columns;

  interface DelimitedValueParser
  {
    List<String> parseLine(String line) throws IOException;
  }

  DelimitedValueReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      @Nullable String listDelimiter,
      @Nullable List<String> columns,
      boolean findColumnsFromHeader,
      int skipHeaderRows,
      DelimitedValueParser parser
  )
  {
    super(inputRowSchema, source);
    this.findColumnsFromHeader = findColumnsFromHeader;
    this.skipHeaderRows = skipHeaderRows;
    final String finalListDelimeter = listDelimiter == null ? Parsers.DEFAULT_LIST_DELIMITER : listDelimiter;
    this.multiValueFunction = ParserUtils.getMultiValueFunction(finalListDelimeter, Splitter.on(finalListDelimeter));
    this.columns = findColumnsFromHeader ? null : columns; // columns will be overriden by header row
    this.parser = parser;
  }

  @Override
  public List<InputRow> parseInputRows(String line) throws IOException, ParseException
  {
    final Map<String, Object> zipped = parseLine(line);
    return Collections.singletonList(MapInputRowParser.parse(getInputRowSchema(), zipped));
  }

  @Override
  public Map<String, Object> toMap(String intermediateRow) throws IOException
  {
    return parseLine(intermediateRow);
  }

  private Map<String, Object> parseLine(String line) throws IOException
  {
    final List<String> parsed = parser.parseLine(line);
    return Utils.zipMapPartial(
        Preconditions.checkNotNull(columns, "columns"),
        Iterables.transform(parsed, multiValueFunction)
    );
  }

  @Override
  public int getNumHeaderLinesToSkip()
  {
    return skipHeaderRows;
  }

  @Override
  public boolean needsToProcessHeaderLine()
  {
    return findColumnsFromHeader;
  }

  @Override
  public void processHeaderLine(String line) throws IOException
  {
    if (!findColumnsFromHeader) {
      throw new ISE("Don't call this if findColumnsFromHeader = false");
    }
    columns = findOrCreateColumnNames(parser.parseLine(line));
    if (columns.isEmpty()) {
      throw new ISE("Empty columns");
    }
  }
}
