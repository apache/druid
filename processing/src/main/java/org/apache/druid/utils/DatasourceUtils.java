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

package org.apache.druid.utils;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;

import java.util.List;

public class DatasourceUtils
{
  private DatasourceUtils()
  {
  }

  public static boolean queryHasTimeFilter(Query<?> query)
  {
    DataSource dataSource = query.getDataSource();
    if (dataSource instanceof JoinDataSource) {
      return joinDataSourceQueryHasTimeFilter(query);
    }
    return dataSource.hasTimeFilter() || isIntervalNonEternity(findBaseDataSourceIntervals(query));
  }

  private static List<Interval> findBaseDataSourceIntervals(Query<?> query)
  {
    return query.getDataSource().getAnalysis()
                .getBaseQuerySegmentSpec()
                .map(QuerySegmentSpec::getIntervals)
                .orElseGet(query::getIntervals);
  }

  /**
   * Checks if a join query has a valid time filter by inspecting parts of the joins and any subqueries used within.
   * If the left datasource is a Table datasource, we require a timefilter on the top level query and a time filter on
   * the right datasource, else we check for time filter on the left and right datasources making up the join recursively
   */
  private static boolean joinDataSourceQueryHasTimeFilter(Query<?> query)
  {
    Preconditions.checkArgument(query.getDataSource() instanceof JoinDataSource);
    JoinDataSource joinDataSource = (JoinDataSource) query.getDataSource();
    if (joinDataSource.getLeft() instanceof TableDataSource) {
      // Make sure we have a time filter on the base query since we have a concrete TableDataSource on the left
      // And then make sure that right also has a time filter
      if (isIntervalNonEternity(query.getIntervals())) {
        return joinDataSource.getRight().hasTimeFilter();
      } else {
        return false;
      }
    }
    // Top level query does not have a time filter, check if all subqueries have a time filter
    return joinDataSource.hasTimeFilter();
  }

  private static boolean isIntervalNonEternity(List<Interval> intervals)
  {
    return !Intervals.ONLY_ETERNITY.equals(intervals);
  }
}
