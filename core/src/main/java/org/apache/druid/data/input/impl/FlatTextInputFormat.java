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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.Property;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class FlatTextInputFormat implements InputFormat
{
  static class Delimiter
  {
    private final char delimiter;
    private final String literal;

    Delimiter(char delimiter, String literal)
    {
      this.delimiter = delimiter;
      this.literal = literal;
    }

//    private void setDelimiter(String delimiter, String literal)
//    {
//      this.delimiter = (delimiter != null && delimiter.length() > 0) ? delimiter.charAt(0) : '\t';
//      this.literal = literal != null ? literal : "customize separator: " + delimiter;
//    }

    public String getDelimiterAsString()
    {
      return String.valueOf(delimiter);
    }

    public char getDelimiter()
    {
      return delimiter;
    }

    public String getLiteral()
    {
      return literal;
    }
  }

  private final String listDelimiter;
  private final List<String> columns;
  private final boolean findColumnsFromHeader;
  private final int skipHeaderRows;
  private final Delimiter delimiter;

  FlatTextInputFormat(
      @Nullable List<String> columns,
      @Nullable String listDelimiter,
      String stringDelimiter,
      @Nullable Boolean hasHeaderRow,
      @Nullable Boolean findColumnsFromHeader,
      int skipHeaderRows
  )
  {
    this.listDelimiter = listDelimiter;
    this.columns = columns == null ? Collections.emptyList() : columns;
    //noinspection ConstantConditions
    this.findColumnsFromHeader = Checks.checkOneNotNullOrEmpty(
        ImmutableList.of(
            new Property<>("hasHeaderRow", hasHeaderRow),
            new Property<>("findColumnsFromHeader", findColumnsFromHeader)
        )
    ).getValue();
    this.skipHeaderRows = skipHeaderRows;
//    this.stringDelimiter = delimiter == null ? "\t" : delimiter;
    this.delimiter = new Delimiter(Preconditions.checkNotNull(stringDelimiter, "stringDelimiter"), )
    Preconditions.checkArgument(
        this.stringDelimiter.length() == 1,
        "The delimiter should be a single character"
    );
    Preconditions.checkArgument(
        !this.stringDelimiter.equals(listDelimiter),
        "Cannot have same delimiter and list delimiter of [%s]",
        this.delimiter
    );
    if (!this.columns.isEmpty()) {
      for (String column : this.columns) {
        Preconditions.checkArgument(
            !column.contains(this.delimiter.getDelimiterAsString()),
            "Column[%s] has a " + this.delimiter.getLiteral() + ", it cannot",
            column
        );
      }
    } else {
      Preconditions.checkArgument(
          this.findColumnsFromHeader,
          "If columns field is not set, the first row of your data must have your header"
          + " and hasHeaderRow must be set to true."
      );
    }
  }

//  private static Delimiter getFormat(String delimiter)
//  {
//    if (",".equals(delimiter)) {
//      return Delimiter.COMMA;
//    } else if ("\t".equals(delimiter)) {
//      return Delimiter.TAB;
//    } else {
//      Delimiter.CUSTOM.setDelimiter(delimiter, null);
//      return Delimiter.CUSTOM;
//    }
//  }

  public Delimiter getDelimiter()
  {
    return delimiter;
  }

  public List<String> getColumns()
  {
    return columns;
  }

  public String getListDelimiter()
  {
    return listDelimiter;
  }

  public boolean isFindColumnsFromHeader()
  {
    return findColumnsFromHeader;
  }

  public int getSkipHeaderRows()
  {
    return skipHeaderRows;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FlatTextInputFormat that = (FlatTextInputFormat) o;
    return findColumnsFromHeader == that.findColumnsFromHeader &&
           skipHeaderRows == that.skipHeaderRows &&
           Objects.equals(listDelimiter, that.listDelimiter) &&
           Objects.equals(columns, that.columns) &&
           Objects.equals(delimiter, that.delimiter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(listDelimiter, columns, findColumnsFromHeader, skipHeaderRows, delimiter);
  }
}
