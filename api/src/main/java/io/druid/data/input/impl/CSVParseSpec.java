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
import com.google.common.base.Preconditions;
import io.druid.java.util.common.parsers.CSVParser;
import io.druid.java.util.common.parsers.Parser;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 */
public class CSVParseSpec extends ParseSpec
{
  @Nullable private final String listDelimiter;
  @Nullable private final Map<String, String> multiValueDelimiter;
  private final List<String> columns;
  private final boolean hasHeaderRow;
  private final int skipHeaderRows;

  @JsonCreator
  public CSVParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("listDelimiter") @Nullable String listDelimiter,
      @JsonProperty("multiValueDelimiter") @Nullable Map<String, String> multiValueDelimiter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("hasHeaderRow") boolean hasHeaderRow,
      @JsonProperty("skipHeaderRows") int skipHeaderRows
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.listDelimiter = listDelimiter;
    this.multiValueDelimiter = multiValueDelimiter;
    this.columns = columns;
    this.hasHeaderRow = hasHeaderRow;
    this.skipHeaderRows = skipHeaderRows;

    if (columns != null) {
      for (String column : columns) {
        Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
      }
      verify(dimensionsSpec.getDimensionNames());
    } else {
      Preconditions.checkArgument(
          hasHeaderRow,
          "If columns field is not set, the first row of your data must have your header"
          + " and hasHeaderRow must be set to true."
      );
    }
  }

  @Deprecated
  public CSVParseSpec(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      @Nullable String listDelimiter,
      @Nullable Map<String, String> multiValueDelimiter,
      List<String> columns
  )
  {
    this(timestampSpec, dimensionsSpec, listDelimiter, multiValueDelimiter, columns, false, 0);
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
  public boolean isHasHeaderRow()
  {
    return hasHeaderRow;
  }

  @JsonProperty("skipHeaderRows")
  public int getSkipHeaderRows()
  {
    return skipHeaderRows;
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
    return new CSVParser(listDelimiter, multiValueDelimiter, columns, hasHeaderRow, skipHeaderRows);
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new CSVParseSpec(spec, getDimensionsSpec(), listDelimiter, multiValueDelimiter, columns, hasHeaderRow, skipHeaderRows);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new CSVParseSpec(getTimestampSpec(), spec, listDelimiter, multiValueDelimiter, columns, hasHeaderRow, skipHeaderRows);
  }
}
