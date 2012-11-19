/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.indexer.data;

import com.google.common.base.Preconditions;
import com.metamx.common.parsers.DelimitedParser;
import com.metamx.common.parsers.Parser;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 */
public class DelimitedDataSpec implements DataSpec
{
  private final String delimiter;
  private final List<String> columns;
  private final List<String> dimensions;

  @JsonCreator
  public DelimitedDataSpec(
      @JsonProperty("delimiter") String delimiter,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("dimensions") List<String> dimensions
  )
  {
    Preconditions.checkNotNull(columns);
    for (String column : columns) {
      Preconditions.checkArgument(!column.contains(","), "Column[%s] has a comma, it cannot", column);
    }

    this.delimiter = (delimiter == null) ? DelimitedParser.DEFAULT_DELIMITER : delimiter;
    this.columns = columns;
    this.dimensions = dimensions;
  }

  @JsonProperty("delimiter")
  public String getDelimiter()
  {
    return delimiter;
  }

  @JsonProperty("columns")
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty("dimensions")
  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
  public void verify(List<String> usedCols)
  {
    for (String columnName : usedCols) {
      Preconditions.checkArgument(columns.contains(columnName), "column[%s] not in columns.", columnName);
    }
  }

  @Override
  public boolean hasCustomDimensions()
  {
    return !(dimensions == null || dimensions.isEmpty());
  }

  @Override
  public Parser<String, Object> getParser()
  {
    Parser<String, Object> retVal = new DelimitedParser(delimiter);
    retVal.setFieldNames(columns);
    return retVal;
  }
}
