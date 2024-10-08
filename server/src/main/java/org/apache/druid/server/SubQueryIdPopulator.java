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

package org.apache.druid.server;

import org.apache.commons.lang3.StringUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

public class SubQueryIdPopulator
{

  public static <T> Query<T> populateSubQueryIds(final Query<T> query)
  {
    return query.withDataSource(generateSubqueryIds(
        query.getDataSource(),
        query.getId(),
        query.getSqlQueryId()
    ));
  }

  /**
   * This method returns the datasource by populating all the {@link QueryDataSource} with correct nesting level and
   * sibling order of all the subqueries that are present.
   * It also plumbs parent query's id and sql id in case the subqueries don't have it set by default
   *
   * @param rootDataSource   Datasource whose subqueries need to be populated
   * @param parentQueryId    Parent Query's ID, can be null if it does not need to update this in the subqueries
   * @param parentSqlQueryId Parent Query's SQL Query ID, can be null if it does not need to update this in the subqueries
   * @return DataSource populated with the subqueries
   */
  private static DataSource generateSubqueryIds(
      DataSource rootDataSource,
      @Nullable final String parentQueryId,
      @Nullable final String parentSqlQueryId
  )
  {
    Queue<DataSource> queue = new ArrayDeque<>();
    queue.add(rootDataSource);

    // Performs BFS on the datasource tree to find the nesting level, and the sibling order of the query datasource
    Map<QueryDataSource, Pair<Integer, Integer>> queryDataSourceToSubqueryIds = new HashMap<>();
    int level = 1;
    while (!queue.isEmpty()) {
      int size = queue.size();
      int siblingOrder = 1;
      for (int i = 0; i < size; ++i) {
        DataSource currentDataSource = queue.poll();
        if (currentDataSource == null) { // Shouldn't be encountered
          continue;
        }
        if (currentDataSource instanceof QueryDataSource) {
          queryDataSourceToSubqueryIds.put((QueryDataSource) currentDataSource, new Pair<>(level, siblingOrder));
          ++siblingOrder;
        }
        queue.addAll(currentDataSource.getChildren());
      }
      ++level;
    }
    /*
    Returns the datasource by populating all the subqueries with the id generated in the map above.
    Implemented in a separate function since the methods on datasource and queries return a new datasource/query
     */
    return insertSubqueryIds(rootDataSource, queryDataSourceToSubqueryIds, parentQueryId, parentSqlQueryId);
  }

  /**
   * To be used in conjunction with {@code generateSubqueryIds()} method. This does the actual task of populating the
   * query's id, subQueryId and sqlQueryId
   *
   * @param currentDataSource            The datasource to be populated with the subqueries
   * @param queryDataSourceToSubqueryIds Map of the datasources to their level and sibling order
   * @param parentQueryId                Parent query's id
   * @param parentSqlQueryId             Parent query's sqlQueryId
   * @return Populates the subqueries from the map
   */
  private static DataSource insertSubqueryIds(
      DataSource currentDataSource,
      Map<QueryDataSource, Pair<Integer, Integer>> queryDataSourceToSubqueryIds,
      @Nullable final String parentQueryId,
      @Nullable final String parentSqlQueryId
  )
  {
    if (currentDataSource instanceof QueryDataSource
        && queryDataSourceToSubqueryIds.containsKey(currentDataSource)) {
      QueryDataSource queryDataSource = (QueryDataSource) currentDataSource;
      Pair<Integer, Integer> nestingInfo = queryDataSourceToSubqueryIds.get(queryDataSource);
      String subQueryId = nestingInfo.lhs + "." + nestingInfo.rhs;
      Query<?> query = queryDataSource.getQuery();

      if (StringUtils.isEmpty(query.getSubQueryId())) {
        query = query.withSubQueryId(subQueryId);
      }

      if (StringUtils.isEmpty(query.getId()) && StringUtils.isNotEmpty(parentQueryId)) {
        query = query.withId(parentQueryId);
      }

      if (StringUtils.isEmpty(query.getSqlQueryId()) && StringUtils.isNotEmpty(parentSqlQueryId)) {
        query = query.withSqlQueryId(parentSqlQueryId);
      }

      currentDataSource = new QueryDataSource(query);
    }
    return currentDataSource.withChildren(currentDataSource.getChildren()
                                                           .stream()
                                                           .map(childDataSource -> insertSubqueryIds(
                                                               childDataSource,
                                                               queryDataSourceToSubqueryIds,
                                                               parentQueryId,
                                                               parentSqlQueryId
                                                           ))
                                                           .collect(Collectors.toList()));
  }

}
