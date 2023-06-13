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

package org.apache.druid.msq.sql.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.sql.http.ResultFormat;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class ResultSetInformation
{

  @Nullable
  private final Long totalRows;
  @Nullable
  private final Long totalSize;

  private final ResultFormat resultFormat;

  private final Boolean header;

  @Nullable
  private final List<Object> records;

  @JsonCreator
  public ResultSetInformation(
      @Nullable
      @JsonProperty ResultFormat resultFormat,
      @Nullable
      @JsonProperty Boolean header,
      @JsonProperty @Nullable Long totalRows,
      @JsonProperty @Nullable Long totalSize,
      @JsonProperty("sampleRecords") @Nullable
      List<Object> records
  )
  {
    this.totalRows = totalRows;
    this.totalSize = totalSize;
    this.resultFormat = resultFormat;
    this.header = header;
    this.records = records;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getTotalRows()
  {
    return totalRows;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getTotalSize()
  {
    return totalSize;
  }

  @JsonProperty
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ResultFormat getResultFormat()
  {
    return resultFormat;
  }

  @JsonProperty
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Boolean getHeader()
  {
    return header;
  }

  @Nullable
  @JsonProperty("sampleRecords")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<Object> getRecords()
  {
    return records;
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
    ResultSetInformation that = (ResultSetInformation) o;
    return Objects.equals(totalRows, that.totalRows)
           && Objects.equals(totalSize, that.totalSize)
           && resultFormat == that.resultFormat
           && Objects.equals(header, that.header)
           && Objects.equals(records, that.records);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(totalRows, totalSize, resultFormat, header, records);
  }

  @Override
  public String toString()
  {
    return "ResultSetInformation{" +
           "totalRows=" + totalRows +
           ", totalSize=" + totalSize +
           ", resultFormat=" + resultFormat +
           ", header=" + header +
           ", records=" + records +
           '}';
  }
}

