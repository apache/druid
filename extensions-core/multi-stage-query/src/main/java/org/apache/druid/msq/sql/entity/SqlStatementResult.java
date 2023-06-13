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
import org.apache.druid.msq.sql.SqlStatementState;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class SqlStatementResult
{

  private final String queryId;

  private final SqlStatementState state;

  private final DateTime createdAt;

  @Nullable
  private final List<ColNameAndType> sqlRowSignature;

  @Nullable
  private final Long durationInMs;

  @Nullable
  private final ResultSetInformation resultSetInformation;


  @JsonCreator
  public SqlStatementResult(
      @JsonProperty("queryId")
      String queryId,
      @JsonProperty("state")
      SqlStatementState state,
      @JsonProperty("createdAt")
      DateTime createdAt,
      @Nullable @JsonProperty("schema")
      List<ColNameAndType> sqlRowSignature,
      @Nullable @JsonProperty("durationInMs")
      Long durationInMs,
      @Nullable @JsonProperty("result")
      ResultSetInformation resultSetInformation

  )
  {
    this.queryId = queryId;
    this.state = state;
    this.createdAt = createdAt;
    this.sqlRowSignature = sqlRowSignature;
    this.durationInMs = durationInMs;
    this.resultSetInformation = resultSetInformation;
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
  public List<ColNameAndType> getSqlRowSignature()
  {
    return sqlRowSignature;
  }

  @JsonProperty
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Long getDurationInMs()
  {
    return durationInMs;
  }

  @JsonProperty
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ResultSetInformation getResultSetInformation()
  {
    return resultSetInformation;
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
    SqlStatementResult that = (SqlStatementResult) o;
    return Objects.equals(queryId, that.queryId) && state == that.state && Objects.equals(
        createdAt,
        that.createdAt
    ) && Objects.equals(sqlRowSignature, that.sqlRowSignature) && Objects.equals(
        durationInMs,
        that.durationInMs
    ) && Objects.equals(resultSetInformation, that.resultSetInformation);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryId, state, createdAt, sqlRowSignature, durationInMs, resultSetInformation);
  }

  @Override
  public String toString()
  {
    return "StatementSqlResult{" +
           "queryId='" + queryId + '\'' +
           ", state=" + state +
           ", createdAt=" + createdAt +
           ", sqlRowSignature=" + sqlRowSignature +
           ", durationInMs=" + durationInMs +
           ", resultSetInformation=" + resultSetInformation +
           '}';
  }
}
