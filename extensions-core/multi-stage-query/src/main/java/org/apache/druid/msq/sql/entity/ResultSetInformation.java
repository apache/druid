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
  private final Long numRows;
  @Nullable
  private final Long sizeInBytes;

  @Nullable
  private final ResultFormat resultFormat;

  @Nullable
  private final List<Object> records;

  @Nullable
  private final String dataSource;

  @JsonCreator
  public ResultSetInformation(
      @JsonProperty("resultFormat") @Nullable ResultFormat resultFormat,
      @JsonProperty("numRows") @Nullable Long numRows,
      @JsonProperty("sizeInBytes") @Nullable Long sizeInBytes,
      @JsonProperty("dataSource") @Nullable String dataSource,
      @JsonProperty("sampleRecords") @Nullable
      List<Object> records
  )
  {
    this.numRows = numRows;
    this.sizeInBytes = sizeInBytes;
    this.resultFormat = resultFormat;
    this.dataSource = dataSource;
    this.records = records;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getNumRows()
  {
    return numRows;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getSizeInBytes()
  {
    return sizeInBytes;
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
  public String getDataSource()
  {
    return dataSource;
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
    return Objects.equals(numRows, that.numRows)
           && Objects.equals(sizeInBytes, that.sizeInBytes)
           && resultFormat == that.resultFormat
           && Objects.equals(records, that.records)
           && Objects.equals(dataSource, that.dataSource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(numRows, sizeInBytes, resultFormat, records, dataSource);
  }

  @Override
  public String toString()
  {
    return "ResultSetInformation{" +
           "totalRows=" + numRows +
           ", totalSize=" + sizeInBytes +
           ", resultFormat=" + resultFormat +
           ", records=" + records +
           ", dataSource='" + dataSource + '\'' +
           '}';
  }

}

