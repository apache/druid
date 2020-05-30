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

package org.apache.druid.segment.join.filter.rewrite;

import org.apache.druid.segment.join.JoinableClause;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A candidate is an RHS column that appears in a filter, along with the value being filtered on, plus
 * the joinable clause associated with the table that the RHS column is from.
 */
public class RhsRewriteCandidate
{
  private final boolean isDirectRewrite;
  @Nonnull private final JoinableClause joinableClause;
  private final String rhsColumn;
  @Nullable private final String valueForRewrite;

  public RhsRewriteCandidate(
      @Nonnull JoinableClause joinableClause,
      String rhsColumn,
      @Nullable String valueForRewrite,
      boolean isDirectRewrite
  )
  {
    this.joinableClause = joinableClause;
    this.rhsColumn = rhsColumn;
    this.valueForRewrite = valueForRewrite;
    this.isDirectRewrite = isDirectRewrite;
  }

  @Nonnull
  public JoinableClause getJoinableClause()
  {
    return joinableClause;
  }

  public String getRhsColumn()
  {
    return rhsColumn;
  }

  @Nullable
  public String getValueForRewrite()
  {
    return valueForRewrite;
  }

  /**
   * A direct rewrite occurs when we filter on an RHS column that is also part of a join equicondition.
   *
   * For example, if we have the filter (j.x = 'hello') and the join condition is (y = j.x), we can directly
   * rewrite the j.x filter to (y = 'hello').
   */
  public boolean isDirectRewrite()
  {
    return isDirectRewrite;
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
    RhsRewriteCandidate that = (RhsRewriteCandidate) o;
    return isDirectRewrite == that.isDirectRewrite &&
           joinableClause.equals(that.joinableClause) &&
           Objects.equals(rhsColumn, that.rhsColumn) &&
           Objects.equals(valueForRewrite, that.valueForRewrite);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(isDirectRewrite, joinableClause, rhsColumn, valueForRewrite);
  }
}
