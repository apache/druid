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

package org.apache.druid.msq.indexing.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.Configs;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class MSQResultsReport
{
  /**
   * Like {@link org.apache.druid.segment.column.RowSignature}, but allows duplicate column names for compatibility
   * with SQL (which also allows duplicate column names in query results).
   */
  private final List<ColumnAndType> signature;
  @Nullable
  private final List<SqlTypeName> sqlTypeNames;
  private final List<Object[]> results;
  private final boolean resultsTruncated;

  @JsonCreator
  public MSQResultsReport(
      @JsonProperty("signature") final List<ColumnAndType> signature,
      @JsonProperty("sqlTypeNames") @Nullable final List<SqlTypeName> sqlTypeNames,
      @JsonProperty("results") final List<Object[]> results,
      @JsonProperty("resultsTruncated") final Boolean resultsTruncated
  )
  {
    this.signature = Preconditions.checkNotNull(signature, "signature");
    this.sqlTypeNames = sqlTypeNames;
    this.results = Preconditions.checkNotNull(results, "results");
    this.resultsTruncated = Configs.valueOrDefault(resultsTruncated, false);
  }

  @JsonProperty("signature")
  public List<ColumnAndType> getSignature()
  {
    return signature;
  }

  @Nullable
  @JsonProperty("sqlTypeNames")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<SqlTypeName> getSqlTypeNames()
  {
    return sqlTypeNames;
  }

  @JsonProperty("results")
  public List<Object[]> getResults()
  {
    return results;
  }

  @JsonProperty("resultsTruncated")
  public boolean isResultsTruncated()
  {
    return resultsTruncated;
  }

  public static class ColumnAndType
  {
    private final String name;
    private final ColumnType type;

    @JsonCreator
    public ColumnAndType(
        @JsonProperty("name") String name,
        @JsonProperty("type") ColumnType type
    )
    {
      this.name = name;
      this.type = type;
    }

    @JsonProperty
    public String getName()
    {
      return name;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public ColumnType getType()
    {
      return type;
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
      ColumnAndType that = (ColumnAndType) o;
      return Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(name, type);
    }

    @Override
    public String toString()
    {
      return name + ":" + type;
    }
  }
}
