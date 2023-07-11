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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class FlatTextInputFormat implements InputFormat
{
  private final List<String> columns;
  private final String listDelimiter;
  private final String delimiter;
  private final boolean findColumnsFromHeader;
  private final int skipHeaderRows;

  FlatTextInputFormat(
      @Nullable List<String> columns,
      @Nullable String listDelimiter,
      String delimiter,
      @Nullable Boolean hasHeaderRow,
      @Nullable Boolean findColumnsFromHeader,
      int skipHeaderRows
  )
  {
    this.columns = columns == null ? Collections.emptyList() : columns;
    this.listDelimiter = listDelimiter;
    this.delimiter = Preconditions.checkNotNull(delimiter, "delimiter");
    if (columns == null || columns.isEmpty()) {
      if (hasHeaderRow != null && findColumnsFromHeader != null) {
        // User provided both hasHeaderRow and findColumnsFromHeader.
        throw new IAE("Cannot accept both [findColumnsFromHeader] and [hasHeaderRow]");
      } else if (hasHeaderRow == null && findColumnsFromHeader == null) {
        // User provided neither columns, nor one of the header-related parameters.
        throw new IAE("Either [columns] or [findColumnsFromHeader] must be set");
      } else {
        // User provided one of hasHeaderRow or findColumnsFromHeader. Take the one they provided.
        this.findColumnsFromHeader = hasHeaderRow != null ? hasHeaderRow : findColumnsFromHeader;
      }
    } else {
      this.findColumnsFromHeader = findColumnsFromHeader == null ? false : findColumnsFromHeader;
    }
    this.skipHeaderRows = skipHeaderRows;
    Preconditions.checkArgument(
        !delimiter.equals(listDelimiter),
        "Cannot have same delimiter and list delimiter of [%s]",
        delimiter
    );
    if (!this.columns.isEmpty()) {
      for (String column : this.columns) {
        Preconditions.checkArgument(
            !column.contains(delimiter),
            "Column[%s] cannot have the delimiter[" + delimiter + "] in its name",
            column
        );
      }
    } else {
      Preconditions.checkArgument(
          this.findColumnsFromHeader,
          "If [columns] is not set, the first row of your data must have your header"
          + " and [findColumnsFromHeader] must be set to true."
      );
    }
  }

  @JsonProperty
  public String getDelimiter()
  {
    return delimiter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getColumns()
  {
    return columns;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isFindColumnsFromHeader()
  {
    return findColumnsFromHeader;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
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

  protected String fieldsToString()
  {
    return "FlatTextInputFormat{"
        + "delimiter=\"" + delimiter
        + "\"listDelimiter="
        + listDelimiter == null ? "null" : "\"" + listDelimiter + "\""
        + ", findColumnsFromHeader=" + findColumnsFromHeader
        + ", skipHeaderRows=" + skipHeaderRows
        + ", columns=" + columns
        + "}";
  }
}
