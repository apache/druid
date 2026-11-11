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

import com.google.common.base.Strings;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;

/**
 * Utilities for setting dimensions on {@link ServiceMetricEvent.Builder}.
 */
public class MSQMetrics
{
  /**
   * Value to emit for {@link DruidMetrics#TYPE} in query metrics.
   */
  private static final String QUERY_METRIC_TYPE = "msq";

  private MSQMetrics()
  {
    // No instantiation.
  }

  /**
   * Sets the following dimensions:
   * <ul>
   * <li>id</li>
   * <li>sqlQueryId</li>
   * <li>type</li>
   * <li>engine</li>
   * </ul>
   */
  public static void setQueryDimensions(
      final ServiceMetricEvent.Builder builder,
      final QueryContext queryContext,
      final String engine
  )
  {
    final String sqlQueryId = queryContext.get(QueryContextParameters.SQL_QUERY_ID);
    final String contextQueryId = queryContext.get(QueryContextParameters.QUERY_ID);
    final String queryId = Strings.isNullOrEmpty(contextQueryId) ? sqlQueryId : contextQueryId;
    builder.setDimensionIfNotNull(DruidMetrics.ID, queryId);
    builder.setDimensionIfNotNull(QueryContextParameters.SQL_QUERY_ID.getName(), sqlQueryId);
    builder.setDimension(DruidMetrics.TYPE, QUERY_METRIC_TYPE);
    builder.setDimension(DruidMetrics.ENGINE, engine);
  }
}
