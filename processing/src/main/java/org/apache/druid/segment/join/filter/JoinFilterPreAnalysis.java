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
import org.apache.druid.segment.join.filter.rewrite.JoinFilterPreAnalysisGroup;

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
 * - The original filter that an analysis was performed ons
 * - A list of filter clauses from the original filter's CNF representation that only reference the base table
 * - A list of filter clauses from the original filter's CNF representation that reference RHS join tables
 * - A list of virtual columns that can only be computed post-join
 * - The JoinFilterPreAnalysisGroup that this pre-analysis is associated with.
 */
public class JoinFilterPreAnalysis
{
  private final JoinableClauses joinableClauses;
  private final Filter originalFilter;
  private final List<Filter> normalizedBaseTableClauses;
  private final List<Filter> normalizedJoinTableClauses;
  private final JoinFilterCorrelations correlations;
  private final JoinFilterPreAnalysisGroup myGroup;
  private final List<VirtualColumn> postJoinVirtualColumns;
  private final Equiconditions equiconditions;

  private JoinFilterPreAnalysis(
      final JoinableClauses joinableClauses,
      final Filter originalFilter,
      final List<VirtualColumn> postJoinVirtualColumns,
      final List<Filter> normalizedBaseTableClauses,
      final List<Filter> normalizedJoinTableClauses,
      JoinFilterCorrelations correlations,
      final JoinFilterPreAnalysisGroup myGroup,
      final Equiconditions equiconditions
  )
  {
    this.joinableClauses = joinableClauses;
    this.originalFilter = originalFilter;
    this.postJoinVirtualColumns = postJoinVirtualColumns;
    this.normalizedBaseTableClauses = normalizedBaseTableClauses;
    this.normalizedJoinTableClauses = normalizedJoinTableClauses;
    this.correlations = correlations;
    this.myGroup = myGroup;
    this.equiconditions = equiconditions;
  }

  public JoinableClauses getJoinableClauses()
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
    return correlations.getCorrelationsByFilteringColumn();
  }

  public Map<String, List<JoinFilterColumnCorrelationAnalysis>> getCorrelationsByDirectFilteringColumn()
  {
    return correlations.getCorrelationsByDirectFilteringColumn();
  }

  public boolean isEnableFilterPushDown()
  {
    return myGroup.isEnableFilterPushDown();
  }

  public boolean isEnableFilterRewrite()
  {
    return myGroup.isEnableFilterRewrite();
  }

  public Equiconditions getEquiconditions()
  {
    return equiconditions;
  }

  /**
   * A Builder class to build {@link JoinFilterPreAnalysis}
   */
  public static class Builder
  {
    @Nonnull private final JoinFilterPreAnalysisGroup group;
    @Nonnull private final JoinableClauses joinableClauses;
    @Nullable private final Filter originalFilter;
    @Nullable private List<Filter> normalizedBaseTableClauses;
    @Nullable private List<Filter> normalizedJoinTableClauses;
    @Nullable private JoinFilterCorrelations correlations;
    @Nonnull private final List<VirtualColumn> postJoinVirtualColumns;
    @Nonnull private Equiconditions equiconditions = new Equiconditions(Collections.emptyMap());

    public Builder(
        @Nonnull JoinFilterPreAnalysisGroup group,
        @Nonnull JoinableClauses joinableClauses,
        @Nullable Filter originalFilter,
        @Nonnull List<VirtualColumn> postJoinVirtualColumns
    )
    {
      this.group = group;
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

    public Builder withCorrelations(
        JoinFilterCorrelations correlations
    )
    {
      this.correlations = correlations;
      return this;
    }

    public Equiconditions computeEquiconditionsFromJoinableClauses()
    {
      Map<String, Set<Expr>> equiconditionsMap = new HashMap<>();
      for (JoinableClause clause : joinableClauses.getJoinableClauses()) {
        for (Equality equality : clause.getCondition().getEquiConditions()) {
          Set<Expr> exprsForRhs = equiconditionsMap.computeIfAbsent(
              clause.getPrefix() + equality.getRightColumn(),
              (rhs) -> new HashSet<>()
          );
          exprsForRhs.add(equality.getLeftExpr());
        }
      }
      this.equiconditions = new Equiconditions(equiconditionsMap);
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
          correlations,
          group,
          equiconditions
      );
    }

  }
}

