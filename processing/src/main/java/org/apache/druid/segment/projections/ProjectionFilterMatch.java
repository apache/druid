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

package org.apache.druid.segment.projections;

import com.google.common.collect.Lists;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.IsBooleanFilter;
import org.apache.druid.segment.filter.TrueFilter;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Terminal marker indicating that a {@link Filter} can be dropped because it matches the filter of a projection
 */
public final class ProjectionFilterMatch extends TrueFilter
{
  static final ProjectionFilterMatch INSTANCE = new ProjectionFilterMatch();

  private ProjectionFilterMatch()
  {
    // no instantiation
  }

  /**
   * Rewrites a query {@link Filter} if possible, removing the {@link Filter} of a projection. To match a projection
   * filter, the query filter must be equal to the projection filter, or must contain the projection filter as the child
   * of an AND filter. This method returns null
   * indicating that a rewrite is impossible with the implication that the query cannot use the projection because the
   * projection doesn't contain all the rows the query would match if not using the projection.
   */
  @Nullable
  public static Filter rewriteFilter(Filter projectionFilter, Filter queryFilter)
  {
    if (queryFilter.equals(projectionFilter)) {
      return ProjectionFilterMatch.INSTANCE;
    }
    if (queryFilter instanceof IsBooleanFilter && ((IsBooleanFilter) queryFilter).isTrue()) {
      final IsBooleanFilter isTrueFilter = (IsBooleanFilter) queryFilter;
      final Filter rewritten = rewriteFilter(projectionFilter, isTrueFilter.getBaseFilter());
      if (rewritten == null) {
        return null;
      }
      //noinspection ObjectEquality
      if (rewritten == ProjectionFilterMatch.INSTANCE) {
        return ProjectionFilterMatch.INSTANCE;
      }
      return new IsBooleanFilter(rewritten, true);
    }
    if (queryFilter instanceof AndFilter) {
      AndFilter andFilter = (AndFilter) queryFilter;

      // if both and filters, check to see if the query and filter contains all of the clauses of the projection and filter
      if (projectionFilter instanceof AndFilter) {
        AndFilter projectionAndFilter = (AndFilter) projectionFilter;
        Filter rewritten = andFilter;
        // calling rewriteFilter using each child of the projection AND filter as the projection filter will remove
        // the child from the query AND filter if it exists (or return null if it does not exist, since it must exist
        // to be a valid rewrite). The remaining AND filter of will only contain children that were not part of the
        // projection AND filter
        for (Filter filter : projectionAndFilter.getFilters()) {
          rewritten = rewriteFilter(filter, rewritten);
          if (rewritten != null) {
            if (rewritten == ProjectionFilterMatch.INSTANCE) {
              return ProjectionFilterMatch.INSTANCE;
            }
          } else {
            return null;
          }
        }
        if (rewritten != null) {
          return rewritten;
        }
        return null;
      }

      // else check to see if any clause of the query AND filter is the projection filter
      List<Filter> newChildren = Lists.newArrayListWithExpectedSize(andFilter.getFilters().size());
      boolean childRewritten = false;
      for (Filter filter : andFilter.getFilters()) {
        Filter rewritten = rewriteFilter(projectionFilter, filter);
        //noinspection ObjectEquality
        if (rewritten == ProjectionFilterMatch.INSTANCE) {
          childRewritten = true;
        } else {
          if (rewritten != null) {
            newChildren.add(rewritten);
            childRewritten = true;
          } else {
            newChildren.add(filter);
          }
        }
      }
      // at least one child must have been rewritten to rewrite the AND
      if (childRewritten) {
        if (newChildren.size() > 1) {
          return new AndFilter(newChildren);
        } else {
          return newChildren.get(0);
        }
      }
      return null;
    }
    return null;
  }
}
