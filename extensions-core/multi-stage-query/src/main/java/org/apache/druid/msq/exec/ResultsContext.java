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

package org.apache.druid.msq.exec;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.run.SqlResults;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Holder for objects needed to interpret SQL results.
 */
public class ResultsContext
{
  private final List<SqlTypeName> sqlTypeNames;
  private final SqlResults.Context sqlResultsContext;

  public ResultsContext(
      final List<SqlTypeName> sqlTypeNames,
      final SqlResults.Context sqlResultsContext
  )
  {
    this.sqlTypeNames = sqlTypeNames;
    this.sqlResultsContext = sqlResultsContext;
  }

  @Nullable
  public List<SqlTypeName> getSqlTypeNames()
  {
    return sqlTypeNames;
  }

  @Nullable
  public SqlResults.Context getSqlResultsContext()
  {
    return sqlResultsContext;
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
    ResultsContext that = (ResultsContext) o;
    return Objects.equals(sqlTypeNames, that.sqlTypeNames)
           && Objects.equals(sqlResultsContext, that.sqlResultsContext);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(sqlTypeNames, sqlResultsContext);
  }

  @Override
  public String toString()
  {
    return "ResultsContext{" +
           "sqlTypeNames=" + sqlTypeNames +
           ", sqlResultsContext=" + sqlResultsContext +
           '}';
  }
}
