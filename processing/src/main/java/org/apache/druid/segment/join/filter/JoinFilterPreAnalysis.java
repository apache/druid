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

package org.apache.druid.segment.join.filter;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.join.Equality;
import org.apache.druid.segment.join.JoinableClause;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A JoinFilterPreAnalysis contains filter push down/rewrite information that does not have per-segment dependencies.
 * This includes:
 * - The query's JoinableClauses list
 * - The query's original filter (if any)
 * - A list of filter clauses from the original filter's CNF representation that only reference the base table
 * - A list of filter clauses from the original filter's CNF representation that reference RHS join tables
 * - A mapping of RHS filtering columns -> List<JoinFilterColumnCorrelationAnalysis>, used for filter rewrites
 * - A second mapping of RHS filtering columns -> List<JoinFilterColumnCorrelationAnalysis>, used for direct filter rewrites
 * - A list of virtual columns that can only be computed post-join
 * - Control flag booleans for whether filter push down and RHS rewrites are enabled.
 */
public class JoinFilterPreAnalysis
{
  private final List<JoinableClause> joinableClauses;
  private final Filter originalFilter;
  private final List<Filter> normalizedBaseTableClauses;
  private final List<Filter> normalizedJoinTableClauses;
  private final Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByFilteringColumn;
  private final Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByDirectFilteringColumn;
  private final boolean enableFilterPushDown;
  private final boolean enableFilterRewrite;
  private final List<VirtualColumn> postJoinVirtualColumns;
  private final Map<String, Set<Expr>> equiconditions;

  private JoinFilterPreAnalysis(
      final List<JoinableClause> joinableClauses,
      final Filter originalFilter,
      final List<VirtualColumn> postJoinVirtualColumns,
      final List<Filter> normalizedBaseTableClauses,
      final List<Filter> normalizedJoinTableClauses,
      final Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByFilteringColumn,
      final Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByDirectFilteringColumn,
      final boolean enableFilterPushDown,
      final boolean enableFilterRewrite,
      final Map<String, Set<Expr>> equiconditions
  )
  {
    this.joinableClauses = joinableClauses;
    this.originalFilter = originalFilter;
    this.postJoinVirtualColumns = postJoinVirtualColumns;
    this.normalizedBaseTableClauses = normalizedBaseTableClauses;
    this.normalizedJoinTableClauses = normalizedJoinTableClauses;
    this.correlationsByFilteringColumn = correlationsByFilteringColumn;
    this.correlationsByDirectFilteringColumn = correlationsByDirectFilteringColumn;
    this.enableFilterPushDown = enableFilterPushDown;
    this.enableFilterRewrite = enableFilterRewrite;
    this.equiconditions = equiconditions;
  }

  public List<JoinableClause> getJoinableClauses()
  {
    return joinableClauses;
  }

  public Filter getOriginalFilter()
  {
    return originalFilter;
  }

  public List<VirtualColumn> getPostJoinVirtualColumns()
  {
    return postJoinVirtualColumns;
  }

  public List<Filter> getNormalizedBaseTableClauses()
  {
    return normalizedBaseTableClauses;
  }

  public List<Filter> getNormalizedJoinTableClauses()
  {
    return normalizedJoinTableClauses;
  }

  public Map<String, List<JoinFilterColumnCorrelationAnalysis>> getCorrelationsByFilteringColumn()
  {
    return correlationsByFilteringColumn;
  }

  public Map<String, List<JoinFilterColumnCorrelationAnalysis>> getCorrelationsByDirectFilteringColumn()
  {
    return correlationsByDirectFilteringColumn;
  }

  public boolean isEnableFilterPushDown()
  {
    return enableFilterPushDown;
  }

  public boolean isEnableFilterRewrite()
  {
    return enableFilterRewrite;
  }

  public Map<String, Set<Expr>> getEquiconditions()
  {
    return equiconditions;
  }

  /**
   * A Builder class to build {@link JoinFilterPreAnalysis}
   */
  public static class Builder
  {
    @Nonnull private final List<JoinableClause> joinableClauses;
    @Nullable private final Filter originalFilter;
    @Nullable private List<Filter> normalizedBaseTableClauses;
    @Nullable private List<Filter> normalizedJoinTableClauses;
    @Nullable private Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByFilteringColumn;
    @Nullable private Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByDirectFilteringColumn;
    private boolean enableFilterPushDown = false;
    private boolean enableFilterRewrite = false;
    @Nonnull private final List<VirtualColumn> postJoinVirtualColumns;
    @Nonnull private Map<String, Set<Expr>> equiconditions = Collections.emptyMap();

    public Builder(
        @Nonnull List<JoinableClause> joinableClauses,
        @Nullable Filter originalFilter,
        @Nonnull List<VirtualColumn> postJoinVirtualColumns
    )
    {
      this.joinableClauses = joinableClauses;
      this.originalFilter = originalFilter;
      this.postJoinVirtualColumns = postJoinVirtualColumns;
    }

    public Builder withNormalizedBaseTableClauses(List<Filter> normalizedBaseTableClauses)
    {
      this.normalizedBaseTableClauses = normalizedBaseTableClauses;
      return this;
    }

    public Builder withNormalizedJoinTableClauses(List<Filter> normalizedJoinTableClauses)
    {
      this.normalizedJoinTableClauses = normalizedJoinTableClauses;
      return this;
    }

    public Builder withCorrelationsByFilteringColumn(
        Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByFilteringColumn
    )
    {
      this.correlationsByFilteringColumn = correlationsByFilteringColumn;
      return this;
    }

    public Builder withCorrelationsByDirectFilteringColumn(
        Map<String, List<JoinFilterColumnCorrelationAnalysis>> correlationsByDirectFilteringColumn
    )
    {
      this.correlationsByDirectFilteringColumn = correlationsByDirectFilteringColumn;
      return this;
    }

    public Builder withEnableFilterPushDown(boolean enableFilterPushDown)
    {
      this.enableFilterPushDown = enableFilterPushDown;
      return this;
    }

    public Builder withEnableFilterRewrite(boolean enableFilterRewrite)
    {
      this.enableFilterRewrite = enableFilterRewrite;
      return this;
    }

    public Map<String, Set<Expr>> computeEquiconditionsFromJoinableClauses()
    {
      this.equiconditions = new HashMap<>();
      for (JoinableClause clause : joinableClauses) {
        for (Equality equality : clause.getCondition().getEquiConditions()) {
          Set<Expr> exprsForRhs = equiconditions.computeIfAbsent(
              clause.getPrefix() + equality.getRightColumn(),
              (rhs) -> new HashSet<>()
          );
          exprsForRhs.add(equality.getLeftExpr());
        }
      }
      return equiconditions;
    }

    public JoinFilterPreAnalysis build()
    {
      return new JoinFilterPreAnalysis(
          joinableClauses,
          originalFilter,
          postJoinVirtualColumns,
          normalizedBaseTableClauses,
          normalizedJoinTableClauses,
          correlationsByFilteringColumn,
          correlationsByDirectFilteringColumn,
          enableFilterPushDown,
          enableFilterRewrite,
          equiconditions
      );
    }

  }
}

