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

package org.apache.druid.query.lookup.namespace.parsers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.DelimitedParser;
import org.apache.druid.java.util.common.parsers.Parser;

import java.util.List;
import java.util.Objects;

@JsonTypeName("tsv")
public class TSVFlatDataParser implements FlatDataParser
{
  private final Parser<String, String> parser;
  private final List<String> columns;
  private final String delimiter;
  private final String listDelimiter;
  private final String keyColumn;
  private final String valueColumn;

  @JsonCreator
  public TSVFlatDataParser(
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("delimiter") String delimiter,
      @JsonProperty("listDelimiter") String listDelimiter,
      @JsonProperty("keyColumn") final String keyColumn,
      @JsonProperty("valueColumn") final String valueColumn,
      @JsonProperty("hasHeaderRow") boolean hasHeaderRow,
      @JsonProperty("skipHeaderRows") int skipHeaderRows
  )
  {
    Preconditions.checkArgument(
        Preconditions.checkNotNull(columns, "`columns` list required").size() > 1,
        "Must specify more than one column to have a key value pair"
    );
    final DelimitedParser delegate = new DelimitedParser(
        StringUtils.emptyToNullNonDruidDataString(delimiter),
        StringUtils.emptyToNullNonDruidDataString(listDelimiter),
        hasHeaderRow,
        skipHeaderRows
    );
    Preconditions.checkArgument(
        !(Strings.isNullOrEmpty(keyColumn) ^ Strings.isNullOrEmpty(valueColumn)),
        "Must specify both `keyColumn` and `valueColumn` or neither `keyColumn` nor `valueColumn`"
    );
    delegate.setFieldNames(columns);
    this.columns = columns;
    this.delimiter = delimiter;
    this.listDelimiter = listDelimiter;
    this.keyColumn = Strings.isNullOrEmpty(keyColumn) ? columns.get(0) : keyColumn;
    this.valueColumn = Strings.isNullOrEmpty(valueColumn) ? columns.get(1) : valueColumn;
    Preconditions.checkArgument(
        columns.contains(this.keyColumn),
        "Column [%s] not found int columns: %s",
        this.keyColumn,
        columns
    );
    Preconditions.checkArgument(
        columns.contains(this.valueColumn),
        "Column [%s] not found int columns: %s",
        this.valueColumn,
        columns
    );

    this.parser = new DelegateParser(delegate, this.keyColumn, this.valueColumn);
  }

  @VisibleForTesting
  public TSVFlatDataParser(
      List<String> columns,
      String delimiter,
      String listDelimiter,
      String keyColumn,
      String valueColumn
  )
  {
    this(columns, delimiter, listDelimiter, keyColumn, valueColumn, false, 0);
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public String getKeyColumn()
  {
    return this.keyColumn;
  }

  @JsonProperty
  public String getValueColumn()
  {
    return this.valueColumn;
  }

  @JsonProperty
  public String getListDelimiter()
  {
    return listDelimiter;
  }

  @JsonProperty
  public String getDelimiter()
  {
    return delimiter;
  }

  @Override
  public Parser<String, String> getParser()
  {
    return parser;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TSVFlatDataParser that = (TSVFlatDataParser) o;
    return Objects.equals(columns, that.columns) &&
           Objects.equals(delimiter, that.delimiter) &&
           Objects.equals(listDelimiter, that.listDelimiter) &&
           Objects.equals(keyColumn, that.keyColumn) &&
           Objects.equals(valueColumn, that.valueColumn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columns, delimiter, listDelimiter, keyColumn, valueColumn);
  }

  @Override
  public String toString()
  {
    return "TSVFlatDataParser{" +
           "columns=" + columns +
           ", delimiter='" + delimiter + '\'' +
           ", listDelimiter='" + listDelimiter + '\'' +
           ", keyColumn='" + keyColumn + '\'' +
           ", valueColumn='" + valueColumn + '\'' +
           '}';
  }
}
