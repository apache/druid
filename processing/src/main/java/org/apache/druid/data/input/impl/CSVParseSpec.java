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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.parsers.CSVParser;
import org.apache.druid.java.util.common.parsers.Parser;

import java.util.List;

import javax.annotation.Nullable;

/**
 */
public class CSVParseSpec extends ParseSpec
{
  private final String listDelimiter;
  private final List<String> columns;
  private final boolean hasHeaderRow;
  private final int skipHeaderRows;
  private final boolean shouldParseNumbers;

  @JsonCreator
  public CSVParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("hasHeaderRow") boolean hasHeaderRow,
      @JsonProperty("skipHeaderRows") int skipHeaderRows,
      @JsonProperty("shouldParseNumbers") @Nullable Boolean shouldParseNumbers
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.listDelimiter = listDelimiter;
    this.columns = columns;
    this.hasHeaderRow = hasHeaderRow;
    this.skipHeaderRows = skipHeaderRows;
    this.shouldParseNumbers = shouldParseNumbers == null ? false : shouldParseNumbers;

    if (columns != null) {
      for (String column : columns) {
        Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
      }
    } else {
      Preconditions.checkArgument(
          hasHeaderRow,
          "If columns field is not set, the first row of your data must have your header"
          + " and hasHeaderRow must be set to true."
      );
    }
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

  @JsonProperty("shouldParseNumbers")
  public boolean shouldParseNumbers()
  {
    return shouldParseNumbers;
  }


  @Override
  public Parser<String, Object> makeParser()
  {
    return new CSVParser(listDelimiter, columns, hasHeaderRow, skipHeaderRows, shouldParseNumbers);
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new CSVParseSpec(spec, getDimensionsSpec(), listDelimiter, columns, hasHeaderRow, skipHeaderRows, shouldParseNumbers);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new CSVParseSpec(getTimestampSpec(), spec, listDelimiter, columns, hasHeaderRow, skipHeaderRows, shouldParseNumbers);
  }
}
