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
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.sql.SqlStatementState;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;

public class SqlStatementResult
{

  private final String queryId;

  private final SqlStatementState state;

  private final DateTime createdAt;

  @Nullable
  private final List<ColumnNameAndTypes> sqlRowSignature;

  @Nullable
  private final Long durationMs;

  @Nullable
  private final ResultSetInformation resultSetInformation;

  @Nullable
  private final ErrorResponse errorResponse;

  @Nullable
  private final MSQStagesReport stages;

  @Nullable
  private final CounterSnapshotsTree counters;

  @Nullable
  private final List<MSQErrorReport> warnings;

  public SqlStatementResult(
      String queryId,
      SqlStatementState state,
      DateTime createdAt,
      List<ColumnNameAndTypes> sqlRowSignature,
      Long durationMs,
      ResultSetInformation resultSetInformation,
      ErrorResponse errorResponse
  )
  {
    this(queryId, state, createdAt, sqlRowSignature, durationMs, resultSetInformation, errorResponse, null, null, null);
  }

  @JsonCreator
  public SqlStatementResult(
      @JsonProperty("queryId")
      String queryId,
      @JsonProperty("state")
      SqlStatementState state,
      @JsonProperty("createdAt")
      DateTime createdAt,
      @Nullable @JsonProperty("schema")
      List<ColumnNameAndTypes> sqlRowSignature,
      @Nullable @JsonProperty("durationMs")
      Long durationMs,
      @Nullable @JsonProperty("result")
      ResultSetInformation resultSetInformation,
      @Nullable @JsonProperty("errorDetails")
      ErrorResponse errorResponse,
      @Nullable @JsonProperty("stages")
      MSQStagesReport stages,
      @Nullable @JsonProperty("counters")
      CounterSnapshotsTree counters,
      @Nullable @JsonProperty("warnings")
      List<MSQErrorReport> warnings
  )
  {
    this.queryId = queryId;
    this.state = state;
    this.createdAt = createdAt;
    this.sqlRowSignature = sqlRowSignature;
    this.durationMs = durationMs;
    this.resultSetInformation = resultSetInformation;
    this.errorResponse = errorResponse;
    this.stages = stages;
    this.counters = counters;
    this.warnings = warnings;
  }

  @JsonProperty
  public String getQueryId()
  {
    return queryId;
  }

  @JsonProperty
  public SqlStatementState getState()
  {
    return state;
  }

  @JsonProperty
  public DateTime getCreatedAt()
  {
    return createdAt;
  }

  @JsonProperty("schema")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<ColumnNameAndTypes> getSqlRowSignature()
  {
    return sqlRowSignature;
  }

  @JsonProperty
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getDurationMs()
  {
    return durationMs;
  }

  @JsonProperty("result")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ResultSetInformation getResultSetInformation()
  {
    return resultSetInformation;
  }

  @JsonProperty("errorDetails")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ErrorResponse getErrorResponse()
  {
    return errorResponse;
  }

  @JsonProperty("stages")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public MSQStagesReport getStages()
  {
    return stages;
  }

  @JsonProperty("counters")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public CounterSnapshotsTree getCounters()
  {
    return counters;
  }

  @JsonProperty("warnings")
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<MSQErrorReport> getWarnings()
  {
    return warnings;
  }

  @Override
  public String toString()
  {
    return "SqlStatementResult{" +
           "queryId='" + queryId + '\'' +
           ", state=" + state +
           ", createdAt=" + createdAt +
           ", sqlRowSignature=" + sqlRowSignature +
           ", durationInMs=" + durationMs +
           ", resultSetInformation=" + resultSetInformation +
           ", errorResponse=" + (errorResponse == null
                                 ? "{}"
                                 : errorResponse.getAsMap().toString()) +
           ", stages=" + stages +
           ", counters=" + counters +
           ", warnings=" + warnings +
           '}';
  }
}
