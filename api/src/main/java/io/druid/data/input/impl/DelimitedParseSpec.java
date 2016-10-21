/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import io.druid.java.util.common.parsers.DelimitedParser;
import io.druid.java.util.common.parsers.Parser;

import java.util.List;

/**
 */
public class DelimitedParseSpec extends ParseSpec
{
  private final String delimiter;
  private final String listDelimiter;
  private final List<String> columns;

  @JsonCreator
  public DelimitedParseSpec(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("delimiter") String delimiter,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("columns") List<String> columns
  )
  {
    super(timestampSpec, dimensionsSpec);

    this.delimiter = delimiter;
    this.listDelimiter = listDelimiter;
    Preconditions.checkNotNull(columns, "columns");
    this.columns = columns;
    for (String column : this.columns) {
      Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
    }

    verify(dimensionsSpec.getDimensionNames());
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
    Parser<String, Object> retVal = new DelimitedParser(
        Optional.fromNullable(delimiter),
        Optional.fromNullable(listDelimiter)
    );
    retVal.setFieldNames(columns);
    return retVal;
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    return new DelimitedParseSpec(spec, getDimensionsSpec(), delimiter, listDelimiter, columns);
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return new DelimitedParseSpec(getTimestampSpec(), spec, delimiter, listDelimiter, columns);
  }

  public ParseSpec withDelimiter(String delim)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delim, listDelimiter, columns);
  }

  public ParseSpec withListDelimiter(String delim)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delimiter, delim, columns);
  }

  public ParseSpec withColumns(List<String> cols)
  {
    return new DelimitedParseSpec(getTimestampSpec(), getDimensionsSpec(), delimiter, listDelimiter, cols);
  }
}
