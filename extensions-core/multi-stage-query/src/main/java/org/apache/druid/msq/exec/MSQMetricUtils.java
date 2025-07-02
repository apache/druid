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

import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QueryContext;
import org.joda.time.Interval;

import java.util.HashSet;
import java.util.Set;

/**
 * Utility methods for setting up dimensions on metrics.
 */
public class MSQMetricUtils
{
  /**
   * Sets dimensions for Dart queries, as well as some common dimensions. Sets the following dimensions:
   * <ul>
   * <li>queryId</li>
   * <li>sqlQueryId</li>
   * <li>type</li>
   * <li>engine</li>
   * </ul>
   */
  public static void setDartDimensions(
      final ServiceMetricEvent.Builder metricBuilder,
      final QueryContext queryContext
  )
  {
    setQueryIdDimensions(metricBuilder, queryContext);
    metricBuilder.setDimension(DruidMetrics.ENGINE, DartSqlEngine.NAME);
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
  public static void setTaskDimensions(
      final ServiceMetricEvent.Builder metricBuilder,
      final Task task,
      final QueryContext queryContext
  )
  {
    setQueryIdDimensions(metricBuilder, queryContext);
    IndexTaskUtils.setTaskDimensions(metricBuilder, task); // Setup common non-MSQ task dimensions
    metricBuilder.setDimension(DruidMetrics.ENGINE, MSQTaskSqlEngine.NAME);
  }

  /**
   * Returns a set of all datasources of all {@link TableInputSpec} in the stageDefinition.
   */
  public static Set<String> getDatasources(final StageDefinition stageDefinition)
  {
    HashSet<String> datasources = new HashSet<>();
    for (InputSpec inputSpec : stageDefinition.getInputSpecs()) {
      if (inputSpec instanceof TableInputSpec) {
        TableInputSpec tableInputSpec = (TableInputSpec) inputSpec;
        datasources.add(tableInputSpec.getDataSource());
      }
    }
    return datasources;
  }

  /**
   * Returns a set of all intervals of all {@link TableInputSpec} in the stageDefinition.
   */
  public static Set<Interval> getIntervals(final StageDefinition stageDefinition)
  {
    HashSet<Interval> intervals = new HashSet<>();
    for (InputSpec inputSpec : stageDefinition.getInputSpecs()) {
      if (inputSpec instanceof TableInputSpec) {
        TableInputSpec tableInputSpec = (TableInputSpec) inputSpec;
        intervals.addAll(tableInputSpec.getIntervals());
      }
    }
    return intervals;
  }

  private static void setQueryIdDimensions(
      final ServiceMetricEvent.Builder metricBuilder,
      final QueryContext queryContext
  )
  {
    metricBuilder.setDimension(BaseQuery.QUERY_ID, queryContext.get(BaseQuery.QUERY_ID));
    metricBuilder.setDimension(BaseQuery.SQL_QUERY_ID, queryContext.get(BaseQuery.SQL_QUERY_ID));
    metricBuilder.setDimension(DruidMetrics.TYPE, "msq");
  }
}
