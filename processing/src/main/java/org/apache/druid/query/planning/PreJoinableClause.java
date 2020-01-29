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

package org.apache.druid.query.planning;

import com.google.common.base.Preconditions;
import org.apache.druid.query.DataSource;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.Joinables;

import java.util.Objects;

/**
 * Like {@link org.apache.druid.segment.join.JoinableClause}, but contains a {@link DataSource} instead of a
 * {@link org.apache.druid.segment.join.Joinable}. This is useful because when analyzing joins, we don't want to
 * actually create Joinables, since that can be an expensive operation.
 */
public class PreJoinableClause
{
  private final String prefix;
  private final DataSource dataSource;
  private final JoinType joinType;
  private final JoinConditionAnalysis condition;

  public PreJoinableClause(
      final String prefix,
      final DataSource dataSource,
      final JoinType joinType,
      final JoinConditionAnalysis condition
  )
  {
    this.prefix = Joinables.validatePrefix(prefix);
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.joinType = Preconditions.checkNotNull(joinType, "joinType");
    this.condition = Preconditions.checkNotNull(condition, "condition");
  }

  public String getPrefix()
  {
    return prefix;
  }

  public DataSource getDataSource()
  {
    return dataSource;
  }

  public JoinType getJoinType()
  {
    return joinType;
  }

  public JoinConditionAnalysis getCondition()
  {
    return condition;
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
    PreJoinableClause that = (PreJoinableClause) o;
    return Objects.equals(prefix, that.prefix) &&
           Objects.equals(dataSource, that.dataSource) &&
           joinType == that.joinType &&
           Objects.equals(condition, that.condition);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(prefix, dataSource, joinType, condition);
  }

  @Override
  public String toString()
  {
    return "JoinClause{" +
           "prefix='" + prefix + '\'' +
           ", dataSource=" + dataSource +
           ", joinType=" + joinType +
           ", condition=" + condition +
           '}';
  }
}
