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

import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.filter.Equiconditions;
import org.apache.druid.segment.join.filter.JoinableClauses;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class RhsRewriteCandidates
{
  private final Set<RhsRewriteCandidate> rhsRewriteCandidates;

  private RhsRewriteCandidates(Set<RhsRewriteCandidate> rhsRewriteCandidates)
  {
    this.rhsRewriteCandidates = rhsRewriteCandidates;
  }

  public Set<RhsRewriteCandidate> getRhsRewriteCandidates()
  {
    return rhsRewriteCandidates;
  }

  /**
   * Determine candidates for filter rewrites.
   * A candidate is an RHS column that appears in a filter, along with the value being filtered on, plus
   * the joinable clause associated with the table that the RHS column is from.
   *
   * These candidates are redued to filter rewrite correlations.
   *
   * @param normalizedJoinTableClauses
   * @param equiconditions
   * @param joinableClauses
   * @return A set of candidates for filter rewrites.
   */
  public static RhsRewriteCandidates getRhsRewriteCandidates(
      List<Filter> normalizedJoinTableClauses,
      Equiconditions equiconditions,
      JoinableClauses joinableClauses)
  {
    Set<RhsRewriteCandidate> rhsRewriteCandidates = new LinkedHashSet<>();
    for (Filter orClause : normalizedJoinTableClauses) {
      if (Filters.filterMatchesNull(orClause)) {
        continue;
      }

      if (orClause instanceof OrFilter) {
        for (Filter subFilter : ((OrFilter) orClause).getFilters()) {
          Optional<RhsRewriteCandidate> rhsRewriteCandidate = determineRhsRewriteCandidatesForSingleFilter(
              subFilter,
              equiconditions,
              joinableClauses
          );

          rhsRewriteCandidate.ifPresent(rhsRewriteCandidates::add);
        }
        continue;
      }

      Optional<RhsRewriteCandidate> rhsRewriteCandidate = determineRhsRewriteCandidatesForSingleFilter(
          orClause,
          equiconditions,
          joinableClauses
      );

      rhsRewriteCandidate.ifPresent(rhsRewriteCandidates::add);
    }
    return new RhsRewriteCandidates(rhsRewriteCandidates);
  }

  private static Optional<RhsRewriteCandidate> determineRhsRewriteCandidatesForSingleFilter(
      Filter orClause,
      Equiconditions equiconditions,
      JoinableClauses joinableClauses
  )
  {
    // Check if the filter clause is on the RHS join column. If so, we can rewrite the clause to filter on the
    // LHS join column instead.
    // Currently, we only support rewrites of filters that operate on a single column for simplicity.
    if (equiconditions.doesFilterSupportDirectJoinFilterRewrite(orClause)) {
      String reqColumn = orClause.getRequiredColumns().iterator().next();
      JoinableClause joinableClause = joinableClauses.getColumnFromJoinIfExists(reqColumn);
      if (joinableClause != null) {
        return Optional.of(
            new RhsRewriteCandidate(
                joinableClause,
                reqColumn,
                null,
                true
            )
        );
      }
    } else if (orClause instanceof SelectorFilter) {
      // this is a candidate for RHS filter rewrite, determine column correlations and correlated values
      String reqColumn = ((SelectorFilter) orClause).getDimension();
      String reqValue = ((SelectorFilter) orClause).getValue();
      JoinableClause joinableClause = joinableClauses.getColumnFromJoinIfExists(reqColumn);
      if (joinableClause != null) {
        return Optional.of(
            new RhsRewriteCandidate(
                joinableClause,
                reqColumn,
                reqValue,
                false
            )
        );
      }
    }

    return Optional.empty();
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
    RhsRewriteCandidates that = (RhsRewriteCandidates) o;
    return Objects.equals(rhsRewriteCandidates, that.rhsRewriteCandidates);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rhsRewriteCandidates);
  }
}
