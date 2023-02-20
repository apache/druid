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
import org.apache.druid.java.util.common.parsers.DelimitedParser;
import org.apache.druid.java.util.common.parsers.Parser;

import java.util.List;

/**
 */
public class DelimitedParseSpec extends ParseSpec
{
  private final String delimiter;
  private final String listDelimiter;
  private final List<String> columns;
  private final boolean hasHeaderRow;
  private final int skipHeaderRows;

  @JsonCreator
  public DelimitedParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("delimiter") String delimiter,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("hasHeaderRow") boolean hasHeaderRow,
      @JsonProperty("skipHeaderRows") int skipHeaderRows
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.delimiter = delimiter;
    this.listDelimiter = listDelimiter;
    this.columns = columns;
    this.hasHeaderRow = hasHeaderRow;
    this.skipHeaderRows = skipHeaderRows;

    if (columns != null) {
      for (String column : this.columns) {
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

  @Deprecated
  public DelimitedParseSpec(
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      String delimiter,
      String listDelimiter,
      List<String> columns
  )
  {
    this(timestampSpec, dimensionsSpec, delimiter, listDelimiter, columns, false, 0);
  }

  @JsonProperty("delimiter")
  public String getDelimiter()
  {
    return delimiter;
  }

  @JsonProperty("listDelimiter")
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
  public Parser<String, Object> makeParser()
  {
    return new DelimitedParser(
        delimiter,
        listDelimiter,
        columns,
        hasHeaderRow,
        skipHeaderRows
    );
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new DelimitedParseSpec(
        spec,
        getDimensionsSpec(),
        delimiter,
        listDelimiter,
        columns,
        hasHeaderRow,
        skipHeaderRows
    );
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new DelimitedParseSpec(
        getTimestampSpec(),
        spec,
        delimiter,
        listDelimiter,
        columns,
        hasHeaderRow,
        skipHeaderRows
    );
  }

}
