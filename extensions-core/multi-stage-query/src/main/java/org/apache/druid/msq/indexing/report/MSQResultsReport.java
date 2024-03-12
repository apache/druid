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
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MSQResultsReport
{
  private static final Logger log = new Logger(MSQResultsReport.class);
  /**
   * Like {@link org.apache.druid.segment.column.RowSignature}, but allows duplicate column names for compatibility
   * with SQL (which also allows duplicate column names in query results).
   */
  private final List<ColumnAndType> signature;
  @Nullable
  private final List<SqlTypeName> sqlTypeNames;
  private final Yielder<Object[]> resultYielder;
  private final boolean resultsTruncated;

  public MSQResultsReport(
      final List<ColumnAndType> signature,
      @Nullable final List<SqlTypeName> sqlTypeNames,
      final Yielder<Object[]> resultYielder,
      @Nullable Boolean resultsTruncated
  )
  {
    this.signature = Preconditions.checkNotNull(signature, "signature");
    this.sqlTypeNames = sqlTypeNames;
    this.resultYielder = Preconditions.checkNotNull(resultYielder, "resultYielder");
    this.resultsTruncated = Configs.valueOrDefault(resultsTruncated, false);
  }

  /**
   * Method that enables Jackson deserialization.
   */
  @JsonCreator
  static MSQResultsReport fromJson(
      @JsonProperty("signature") final List<ColumnAndType> signature,
      @JsonProperty("sqlTypeNames") @Nullable final List<SqlTypeName> sqlTypeNames,
      @JsonProperty("results") final List<Object[]> results,
      @JsonProperty("resultsTruncated") final Boolean resultsTruncated
  )
  {
    return new MSQResultsReport(signature, sqlTypeNames, Yielders.each(Sequences.simple(results)), resultsTruncated);
  }

  public static MSQResultsReport createReportAndLimitRowsIfNeeded(
      final List<ColumnAndType> signature,
      @Nullable final List<SqlTypeName> sqlTypeNames,
      Yielder<Object[]> resultYielder,
      MSQSelectDestination selectDestination
  )
  {
    List<Object[]> results = new ArrayList<>();
    long rowCount = 0;
    int factor = 1;
    while (!resultYielder.isDone()) {
      results.add(resultYielder.get());
      resultYielder = resultYielder.next(null);
      ++rowCount;
      if (selectDestination.shouldTruncateResultsInTaskReport() && rowCount >= Limits.MAX_SELECT_RESULT_ROWS) {
        break;
      }
      if (rowCount % (factor * Limits.MAX_SELECT_RESULT_ROWS) == 0) {
        log.warn(
            "Task report is getting too large with %d rows. Large task reports can cause the controller to go out of memory. "
            + "Consider using the 'limit %d' clause in your query to reduce the number of rows in the result. "
            + "If you require all the results, consider setting [%s=%s] in the query context which will allow you to fetch large result sets.",
            rowCount,
            Limits.MAX_SELECT_RESULT_ROWS,
            MultiStageQueryContext.CTX_SELECT_DESTINATION,
            MSQSelectDestination.DURABLESTORAGE.getName()
        );
        factor = factor < 32 ? factor * 2 : 32;
      }
    }
    return new MSQResultsReport(
        signature,
        sqlTypeNames,
        Yielders.each(Sequences.simple(results)),
        !resultYielder.isDone()
    );
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
  public Yielder<Object[]> getResultYielder()
  {
    return resultYielder;
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
