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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.parsers.CSVParser;
import io.druid.java.util.common.parsers.Parser;

import java.util.List;

/**
 */
public class CSVParseSpec extends ParseSpec
{
  private final String listDelimiter;
  private final List<String> columns;
  private final boolean firstRowIsHeader;

  @JsonCreator
  public CSVParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("firstRowIsHeader") boolean firstRowIsHeader
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.listDelimiter = listDelimiter;
    this.columns = columns;

    if (columns != null) {
      for (String column : columns) {
        Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
      }
      verify(dimensionsSpec.getDimensionNames());
    } else {
      Preconditions.checkArgument(
          firstRowIsHeader,
          "If columns field is not set, the first row of your data must have your header and firstRowIsHeader must be set to true."
      );
    }

    this.firstRowIsHeader = firstRowIsHeader;
  }

  @JsonProperty
  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @JsonProperty("columns")
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public boolean isFirstRowIsHeader()
  {
    return firstRowIsHeader;
  }

  @Override
  public void verify(List<String> usedCols)
  {
    for (String columnName : usedCols) {
      Preconditions.checkArgument(columns.contains(columnName), "column[%s] not in columns.", columnName);
    }
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    if (firstRowIsHeader) {
      return new CSVParser(Optional.fromNullable(listDelimiter), columns, firstRowIsHeader);
    } else {
      return new CSVParser(Optional.fromNullable(listDelimiter), columns);
    }
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new CSVParseSpec(spec, getDimensionsSpec(), listDelimiter, columns, firstRowIsHeader);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new CSVParseSpec(getTimestampSpec(), spec, listDelimiter, columns, firstRowIsHeader);
  }

  public ParseSpec withColumns(List<String> cols)
  {
    return new CSVParseSpec(getTimestampSpec(), getDimensionsSpec(), listDelimiter, cols, firstRowIsHeader);
  }
}
