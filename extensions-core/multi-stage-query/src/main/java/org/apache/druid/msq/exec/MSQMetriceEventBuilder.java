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

import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QueryContext;

import java.util.Map;

/**
 * Service metric event builder for MSQ.
 */
public class MSQMetriceEventBuilder extends ServiceMetricEvent.Builder
{
  /**
   * Value to emit for {@link DruidMetrics#TYPE} in query metrics.
   */
  private static final String QUERY_METRIC_TYPE = "msq";

  /**
   * Sets dimensions for Dart queries, as well as some common dimensions. Sets the following dimensions:
   * <ul>
   * <li>queryId</li>
   * <li>sqlQueryId</li>
   * <li>type</li>
   * <li>engine</li>
   * </ul>
   */
  public void setDartDimensions(final QueryContext queryContext)
  {
    setQueryIdDimensions(queryContext);
    setDimension(DruidMetrics.ENGINE, DartSqlEngine.NAME);
  }

  /**
   * Sets dimensions for MSQ task queries, as well as some common dimensions. Sets the following dimensions:
   * <ul>
   * <li>queryId</li>
   * <li>sqlQueryId</li>
   * <li>type</li>
   * <li>engine</li>
   * <li>taskId</li>
   * <li>taskType</li>
   * <li>dataSource</li>
   * <li>tags</li>
   * <li>groupId</li>
   * </ul>
   */
  public void setTaskDimensions(final Task task, final QueryContext queryContext)
  {
    setQueryIdDimensions(queryContext);
    setDimension(DruidMetrics.TASK_ID, task.getId());
    setDimension(DruidMetrics.TASK_TYPE, task.getType());

    // For backward compability, set datasource only if it has not already been set.
    if (getDimension(DruidMetrics.DATASOURCE) == null) {
      setDimension(DruidMetrics.DATASOURCE, task.getDataSource());
    }

    setDimensionIfNotNull(
        DruidMetrics.TAGS,
        task.<Map<String, Object>>getContextValue(DruidMetrics.TAGS)
    );
    setDimensionIfNotNull(DruidMetrics.GROUP_ID, task.getGroupId());
    setDimension(DruidMetrics.ENGINE, MSQTaskSqlEngine.NAME);
  }

  private void setQueryIdDimensions(final QueryContext queryContext)
  {
    setDimensionIfNotNull(BaseQuery.QUERY_ID, queryContext.getString(BaseQuery.QUERY_ID));
    setDimensionIfNotNull(BaseQuery.SQL_QUERY_ID, queryContext.getString(BaseQuery.SQL_QUERY_ID));
    setDimension(DruidMetrics.TYPE, QUERY_METRIC_TYPE);
  }
}
