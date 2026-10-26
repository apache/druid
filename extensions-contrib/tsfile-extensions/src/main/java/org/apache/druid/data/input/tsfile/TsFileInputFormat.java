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

package org.apache.druid.data.input.tsfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.File;
import java.util.List;
import java.util.Objects;

/**
 * Reads Apache TsFile files into Druid. TsFile is a columnar file format for time series data, so a single file
 * can hold multiple tables. Rows of each table are converted into {@link org.apache.druid.data.input.InputRow}s.
 * <p>
 * The timestamp of a TsFile row is returned as the {@code Time} column. The ingestion spec must therefore use
 * {@code "timestampSpec": {"column": "Time", "format": "millis"}} (or any other numeric format).
 */
public class TsFileInputFormat implements InputFormat
{
  /** Name of the implicit time column of TsFile query results. */
  public static final String TIME_COLUMN_NAME = "Time";

  @Nullable
  private final List<String> tables;
  @Nullable
  private final List<String> columns;
  @Nullable
  private final Interval interval;

  @JsonCreator
  public TsFileInputFormat(
      @JsonProperty("tables") @Nullable List<String> tables,
      @JsonProperty("columns") @Nullable List<String> columns,
      @JsonProperty("interval") @Nullable Interval interval
  )
  {
    this.tables = tables;
    this.columns = columns;
    this.interval = interval;
  }

  /**
   * Tables to read. When null or empty, all tables in the file are read.
   */
  @Nullable
  @JsonProperty("tables")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getTables()
  {
    return tables;
  }

  /**
   * Columns to read. When null or empty, all columns of each table are read.
   */
  @Nullable
  @JsonProperty("columns")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getColumns()
  {
    return columns;
  }

  /**
   * Time range to read, in Druid's {@code [start, end)} semantics. When null, each file is read in full.
   * The range is pushed down to the TsFile reader, which skips chunks whose time statistics fall outside of it.
   */
  @Nullable
  @JsonProperty("interval")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory
  )
  {
    return new TsFileInputEntityReader(inputRowSchema, source, temporaryDirectory, tables, columns, interval);
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
    TsFileInputFormat that = (TsFileInputFormat) o;
    return Objects.equals(tables, that.tables)
           && Objects.equals(columns, that.columns)
           && Objects.equals(interval, that.interval);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tables, columns, interval);
  }

  @Override
  public String toString()
  {
    return "TsFileInputFormat{" +
           "tables=" + tables +
           ", columns=" + columns +
           ", interval=" + interval +
           '}';
  }
}
