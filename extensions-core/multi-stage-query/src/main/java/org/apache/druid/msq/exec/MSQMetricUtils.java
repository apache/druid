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

import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;

/**
 * Utility methods for setting up dimensions on metrics.
 */
public class MSQMetricUtils
{
  private static void setQueryIdDimensions(
      final ServiceMetricEvent.Builder metricBuilder,
      final QueryContext queryContext
  )
  {
    metricBuilder.setDimension(BaseQuery.QUERY_ID, queryContext.get(BaseQuery.QUERY_ID));
    metricBuilder.setDimension(BaseQuery.SQL_QUERY_ID, queryContext.get(BaseQuery.SQL_QUERY_ID));
  }

  public static void setDartQueryIdDimensions(
      final ServiceMetricEvent.Builder metricBuilder,
      final QueryContext queryContext
  )
  {
    metricBuilder.setDimension(DruidMetrics.ENGINE, DartSqlEngine.NAME);
    setQueryIdDimensions(metricBuilder, queryContext);
    metricBuilder.setDimension(QueryContexts.CTX_DART_QUERY_ID, queryContext.get(QueryContexts.CTX_DART_QUERY_ID));
  }

  public static void setTaskQueryIdDimensions(
      final ServiceMetricEvent.Builder metricBuilder,
      final QueryContext queryContext
  )
  {
    metricBuilder.setDimension(DruidMetrics.ENGINE, MSQTaskSqlEngine.NAME);
    setQueryIdDimensions(metricBuilder, queryContext);
  }
}
