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
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import org.apache.druid.common.config.NullHandling;
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
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class SeparateValueReader extends TextReader
{
  private final boolean findColumnsFromHeader;
  private final int skipHeaderRows;
  private final Function<String, Object> multiValueFunction;
  @Nullable
  private List<String> columns;
  private final SeparateValueInputFormat.Format format;
  private final RFC4180Parser parser;

  public static RFC4180Parser createOpenCsvParser(char separator)
  {
    return NullHandling.replaceWithDefault()
           ? new RFC4180ParserBuilder()
               .withSeparator(separator)
               .build()
           : new RFC4180ParserBuilder().withFieldAsNull(
               CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
                                       .withSeparator(separator)
                                       .build();
  }

  SeparateValueReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory,
      @Nullable String listDelimiter,
      @Nullable List<String> columns,
      boolean findColumnsFromHeader,
      int skipHeaderRows,
      SeparateValueInputFormat.Format format
  )
  {
    super(inputRowSchema, source, temporaryDirectory);
    this.findColumnsFromHeader = findColumnsFromHeader;
    this.skipHeaderRows = skipHeaderRows;
    final String finalListDelimeter = listDelimiter == null ? Parsers.DEFAULT_LIST_DELIMITER : listDelimiter;
    this.multiValueFunction = ParserUtils.getMultiValueFunction(finalListDelimeter, Splitter.on(finalListDelimeter));
    this.columns = findColumnsFromHeader ? null : columns; // columns will be overriden by header row
    this.format = format;
    this.parser = createOpenCsvParser(format.getDelimiter());

    if (this.columns != null) {
      for (String column : this.columns) {
        Preconditions.checkArgument(
            !column.contains(format.getDelimiterAsString()),
            "Column[%s] has a " + format.getLiteral() + ", it cannot",
            column
        );
      }
    } else {
      Preconditions.checkArgument(
          findColumnsFromHeader,
          "If columns field is not set, the first row of your data must have your header"
          + " and hasHeaderRow must be set to true."
      );
    }
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
    final String[] parsed = parser.parseLine(line);
    return Utils.zipMapPartial(
        Preconditions.checkNotNull(columns, "columns"),
        Iterables.transform(Arrays.asList(parsed), multiValueFunction)
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
    columns = findOrCreateColumnNames(Arrays.asList(parser.parseLine(line)));
    if (columns.isEmpty()) {
      throw new ISE("Empty columns");
    }
  }
}
