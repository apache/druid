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

public class SeparateValueInputFormat implements InputFormat
{
  private final String listDelimiter;
  private final List<String> columns;
  private final boolean findColumnsFromHeader;
  private final int skipHeaderRows;
  private final String seperator;

  @JsonCreator
  public SeparateValueInputFormat(
      @JsonProperty("columns") @Nullable List<String> columns,
      @JsonProperty("listDelimiter") @Nullable String listDelimiter,
      @Deprecated @JsonProperty("hasHeaderRow") @Nullable Boolean hasHeaderRow,
      @JsonProperty("findColumnsFromHeader") @Nullable Boolean findColumnsFromHeader,
      @JsonProperty("skipHeaderRows") int skipHeaderRows,
      String seperator
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
    this.seperator = seperator;

    if (!this.columns.isEmpty()) {
      for (String column : this.columns) {
        Preconditions.checkArgument(
            !column.contains("tab".equals(seperator) ? "\t" : ","),
            "Column[%s] has a " + this.seperator + ", it cannot",
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
    return new SeparateValueReader(
        inputRowSchema,
        source,
        temporaryDirectory,
        listDelimiter,
        columns,
        findColumnsFromHeader,
        skipHeaderRows,
        seperator
    );
  }

  public InputEntityReader createReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory,
      String seperator
  )
  {
    return "tab".equals(seperator) ? new TsvReader(
        inputRowSchema,
        source,
        temporaryDirectory,
        listDelimiter,
        columns,
        findColumnsFromHeader,
        skipHeaderRows
    ) : new CsvReader(
        inputRowSchema,
        source,
        temporaryDirectory,
        listDelimiter,
        columns,
        findColumnsFromHeader,
        skipHeaderRows
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
    SeparateValueInputFormat format = (SeparateValueInputFormat) o;
    return findColumnsFromHeader == format.findColumnsFromHeader &&
           skipHeaderRows == format.skipHeaderRows &&
           Objects.equals(listDelimiter, format.listDelimiter) &&
           Objects.equals(columns, format.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(listDelimiter, columns, findColumnsFromHeader, skipHeaderRows);
  }
}
