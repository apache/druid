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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.Property;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * InputFormat for customized Delimitor Separate Value format of input data(default is TSV).
 */
public class DelimitedInputFormat implements InputFormat
{

  public enum Format
  {
    CSV(',', "comma"),
    TSV('\t', "tab"),
    CustomizeSV('|', "");

    private char delimiter;
    private String literal;

    Format(char delimiter, String literal)
    {
      this.delimiter = delimiter;
      this.literal = literal;
    }

    public String getDelimiterAsString()
    {
      return String.valueOf(delimiter);
    }

    private void setDelimiter(String delimiter, String literal)
    {
      this.delimiter = (delimiter != null && delimiter.length() > 0) ? delimiter.charAt(0) : '\t';
      this.literal = literal != null ? literal : "customize separator: " + delimiter;
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
  private final Format format;
  private final String delimiter;

  public DelimitedInputFormat(
      @JsonProperty("columns") @Nullable List<String> columns,
      @JsonProperty("listDelimiter") @Nullable String listDelimiter,
      @JsonProperty("delimiter") @Nullable String delimiter,
      @Deprecated @JsonProperty("hasHeaderRow") @Nullable Boolean hasHeaderRow,
      @JsonProperty("findColumnsFromHeader") @Nullable Boolean findColumnsFromHeader,
      @JsonProperty("skipHeaderRows") int skipHeaderRows
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
    this.delimiter = delimiter == null ? "\t" : delimiter;
    this.format = getFormat(this.delimiter);
    Preconditions.checkArgument(
        this.delimiter.length() == 1,
        "The delimiter should be a single character"
    );
    Preconditions.checkArgument(
        !this.delimiter.equals(listDelimiter),
        "Cannot have same delimiter and list delimiter of [%s]",
        this.delimiter
    );
    if (!this.columns.isEmpty()) {
      for (String column : this.columns) {
        Preconditions.checkArgument(
            !column.contains(format.getDelimiterAsString()),
            "Column[%s] has a " + format.getLiteral() + ", it cannot",
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

  private static Format getFormat(String delimiter)
  {
    if (",".equals(delimiter)) {
      return Format.CSV;
    } else if ("\t".equals(delimiter)) {
      return Format.TSV;
    } else {
      Format.CustomizeSV.setDelimiter(delimiter, null);
      return Format.CustomizeSV;
    }
  }


  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @JsonProperty
  public boolean isFindColumnsFromHeader()
  {
    return findColumnsFromHeader;
  }

  @JsonProperty
  public int getSkipHeaderRows()
  {
    return skipHeaderRows;
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
        temporaryDirectory,
        listDelimiter,
        columns,
        findColumnsFromHeader,
        skipHeaderRows,
        this.format
    );
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
    DelimitedInputFormat format = (DelimitedInputFormat) o;
    return findColumnsFromHeader == format.findColumnsFromHeader &&
           skipHeaderRows == format.skipHeaderRows &&
           Objects.equals(listDelimiter, format.listDelimiter) &&
           Objects.equals(columns, format.columns) &&
           Objects.equals(this.format, format.format);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(listDelimiter, columns, findColumnsFromHeader, skipHeaderRows, format);
  }
}
