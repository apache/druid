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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinPrefixUtils;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableFactoryWrapper;

import javax.annotation.Nullable;
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
  private final JoinAlgorithm joinAlgorithm;

  public PreJoinableClause(final JoinDataSource joinDataSource)
  {
    this.prefix = JoinPrefixUtils.validatePrefix(joinDataSource.getRightPrefix());
    this.dataSource = Preconditions.checkNotNull(joinDataSource.getRight(), "dataSource");
    this.joinType = Preconditions.checkNotNull(joinDataSource.getJoinType(), "joinType");
    this.condition = Preconditions.checkNotNull(joinDataSource.getConditionAnalysis(), "condition");
    this.joinAlgorithm = joinDataSource.getJoinAlgorithm();
  }

  public String getPrefix()
  {
    return prefix;
  }

  public DataSource getDataSource()
  {
    return dataSource;
  }

  /**
   * If the data source is a {@link RestrictedDataSource} with a {@link NoRestrictionPolicy}, unwraps it to a table and return.
   * <p>
   * This is a temporary workaround to allow the planner to work with {@link RestrictedDataSource} as the right-side join.
   */
  public DataSource maybeUnwrapRestrictedDataSource()
  {
    if (dataSource instanceof RestrictedDataSource) {
      RestrictedDataSource restricted = (RestrictedDataSource) dataSource;
      if (restricted.getPolicy() instanceof NoRestrictionPolicy) {
        return restricted.getBase();
      } else {
        throw new QueryUnsupportedException(StringUtils.format(
            "Restricted data source [%s] with policy [%s] is not supported",
            restricted.getBase(),
            restricted.getPolicy()
        ));
      }
    } else {
      return dataSource;
    }
  }

  public JoinType getJoinType()
  {
    return joinType;
  }

  public JoinConditionAnalysis getCondition()
  {
    return condition;
  }

  @Nullable
  public JoinAlgorithm getJoinAlgorithm()
  {
    return joinAlgorithm;
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
    return Objects.equals(prefix, that.prefix)
           && Objects.equals(dataSource, that.dataSource)
           && joinType == that.joinType
           && Objects.equals(condition, that.condition)
           && joinAlgorithm == that.joinAlgorithm;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(prefix, dataSource, joinType, condition, joinAlgorithm);
  }

  @Override
  public String toString()
  {
    return "JoinClause{" +
           "prefix='" + prefix + '\'' +
           ", dataSource=" + dataSource +
           ", joinType=" + joinType +
           ", condition=" + condition +
           ", joinAlgorithm=" + joinAlgorithm +
           '}';
  }

  public JoinDataSource makeUpdatedJoinDataSource(DataSource newSource, DimFilter joinBaseFilter, JoinableFactoryWrapper joinableFactoryWrapper)
  {
    PreJoinableClause clause = this;
    return JoinDataSource.create(
        newSource,
        clause.getDataSource(),
        clause.getPrefix(),
        clause.getCondition(),
        clause.getJoinType(),
        joinBaseFilter,
        joinableFactoryWrapper,
        clause.getJoinAlgorithm()
    );
  }
}
