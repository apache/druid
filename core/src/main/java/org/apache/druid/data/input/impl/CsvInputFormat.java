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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Arrays;
import java.util.List;

public class CsvInputFormat extends FlatTextInputFormat
{
  private static final char SEPARATOR = ',';
  private static final RFC4180Parser PARSER = createOpenCsvParser();

  @JsonCreator
  public CsvInputFormat(
      @JsonProperty("columns") @Nullable List<String> columns,
      @JsonProperty("listDelimiter") @Nullable String listDelimiter,
      @Deprecated @JsonProperty("hasHeaderRow") @Nullable Boolean hasHeaderRow,
      @JsonProperty("findColumnsFromHeader") @Nullable Boolean findColumnsFromHeader,
      @JsonProperty("skipHeaderRows") int skipHeaderRows
  )
  {
    super(columns, listDelimiter, String.valueOf(SEPARATOR), hasHeaderRow, findColumnsFromHeader, skipHeaderRows);
  }

  @Override
  @JsonIgnore
  public String getDelimiter()
  {
    return super.getDelimiter();
  }

  @Override
  public boolean isSplittable()
  {
    return true;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return new DelimitedValueReader(
        inputRowSchema,
        source,
        getListDelimiter(),
        getColumns(),
        isFindColumnsFromHeader(),
        getSkipHeaderRows(),
        line -> Arrays.asList(PARSER.parseLine(line))
    );
  }

  public static RFC4180Parser createOpenCsvParser()
  {
    return NullHandling.replaceWithDefault()
           ? new RFC4180ParserBuilder().withSeparator(SEPARATOR).build()
           : new RFC4180ParserBuilder().withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
                                       .withSeparator(SEPARATOR)
                                       .build();
  }
}
