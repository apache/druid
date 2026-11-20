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

import com.google.inject.Injector;
import org.apache.druid.java.util.emitter.service.ExtraDimensionsServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.context.QueryContextParameters;

import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for MSQ metrics.
 */
public class MSQMetrics
{
  /**
   * Value to emit for {@link DruidMetrics#TYPE} in query metrics when no native query is present.
   */
  public static final String QUERY_METRIC_TYPE = "msq";

  private MSQMetrics()
  {
    // No instantiation.
  }

  /**
   * Sets the following dimensions:
   * <ul>
   * <li>id</li>
   * <li>queryId</li>
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
    final String queryId = queryContext.get(QueryContextParameters.QUERY_ID);
    builder.setDimensionIfNotNull(DruidMetrics.ID, queryId);
    builder.setDimensionIfNotNull(DruidMetrics.QUERY_ID, queryId);
    builder.setDimensionIfNotNull(
        QueryContextParameters.SQL_QUERY_ID.getName(),
        queryContext.get(QueryContextParameters.SQL_QUERY_ID)
    );
    builder.setDimension(DruidMetrics.TYPE, QUERY_METRIC_TYPE);
    builder.setDimension(DruidMetrics.ENGINE, engine);
  }

  /**
   * Returns new {@link QueryMetrics} for a query, using the {@link QueryToolChestWarehouse} and
   * {@link GenericQueryMetricsFactory} from the provided injector.
   */
  public static QueryMetrics<?> makeQueryMetrics(final Injector injector, final Query<?> query, final String engine)
  {
    return makeQueryMetrics(
        injector.getInstance(QueryToolChestWarehouse.class),
        injector.getInstance(GenericQueryMetricsFactory.class),
        query,
        engine
    );
  }

  /**
   * Returns new {@link QueryMetrics} for a query.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static QueryMetrics<?> makeQueryMetrics(
      final QueryToolChestWarehouse warehouse,
      final GenericQueryMetricsFactory genericQueryMetricsFactory,
      final Query<?> query,
      final String engine
  )
  {
    final QueryToolChest toolChest = warehouse.getToolChest((Query) query);
    final QueryMetrics<?> queryMetrics =
        toolChest != null ? toolChest.makeMetrics(query) : genericQueryMetricsFactory.makeMetrics(query);

    // Replace the engine passed by QueryMetrics#query, which is always "native".
    queryMetrics.engine(engine);
    return queryMetrics;
  }

  /**
   * Emits the provided {@link QueryMetrics}, adding {@link DruidMetrics#ENGINE} and the provided extra dimensions to
   * each metric that does not already have them.
   */
  public static void emitQueryMetrics(
      final QueryMetrics<?> queryMetrics,
      final ServiceEmitter emitter,
      final String engine,
      final Map<String, Object> extraDimensions
  )
  {
    final Map<String, Object> dimensions = new HashMap<>(extraDimensions);
    dimensions.put(DruidMetrics.ENGINE, engine);
    queryMetrics.emit(new ExtraDimensionsServiceEmitter(emitter, dimensions));
  }
}
