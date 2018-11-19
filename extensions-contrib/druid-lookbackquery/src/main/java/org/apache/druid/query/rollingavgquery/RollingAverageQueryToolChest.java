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
package org.apache.druid.query.rollingavgquery;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.inject.Inject;
import org.apache.druid.query.rollingavgquery.averagers.AveragerFactory;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.server.log.RequestLogger;

/**
 * The QueryToolChest for RollingAverage Query
 */
public class RollingAverageQueryToolChest extends QueryToolChest<Row, RollingAverageQuery>
{

  private final QuerySegmentWalker walker;
  private final RequestLogger requestLogger;
  private QueryToolChestWarehouse warehouse;

  public static final String ROLLING_AVERAGE_MERGE_KEY = "rollingAverageMerge";

  private final RollingAverageQueryMetricsFactory rollingAverageQueryMetricsFactory;

  /**
   * Construct a RollingAverageQueryToolChest for processing rolling-average queries.
   * RollingAverage queries are expected to be processed on broker nodes and never hit historical nodes.
   *
   * @param walker
   * @param requestLogger
   */
  @Inject
  public RollingAverageQueryToolChest(@Nullable QuerySegmentWalker walker, RequestLogger requestLogger)
  {

    this.walker = walker;
    this.requestLogger = requestLogger;
    this.rollingAverageQueryMetricsFactory = DefaultRollingAverageQueryMetricsFactory.instance();
  }

  @Inject(optional = true)
  public void setWarehouse(QueryToolChestWarehouse warehouse)
  {
    this.warehouse = warehouse;
  }

  @Override
  public QueryRunner<Row> mergeResults(QueryRunner<Row> runner)
  {
    return new RollingAverageQueryRunner(warehouse, walker, requestLogger);
  }

  @Override
  public QueryMetrics<? super RollingAverageQuery> makeMetrics(RollingAverageQuery query)
  {
    RollingAverageQueryMetrics rollingAverageQueryMetrics = rollingAverageQueryMetricsFactory.makeMetrics();
    rollingAverageQueryMetrics.query(query);
    return rollingAverageQueryMetrics;
  }

  @Override
  public Function<Row, Row> makePostComputeManipulatorFn(RollingAverageQuery query, MetricManipulationFn fn)
  {

    return new Function<Row, Row>()
    {

      @Override
      public Row apply(Row result)
      {
        MapBasedRow mRow = (MapBasedRow) result;
        final Map<String, Object> values = new HashMap<>(mRow.getEvent());

        for (AggregatorFactory agg : query.getAggregatorSpecs()) {
          Object aggVal = values.get(agg.getName());
          if (aggVal != null) {
            values.put(agg.getName(), fn.manipulate(agg, aggVal));
          } else {
            values.put(agg.getName(), null);
          }
        }

        for (AveragerFactory<?, ?> avg : query.getAveragerSpecs()) {
          Object aggVal = values.get(avg.getName());
          if (aggVal != null) {
            values.put(avg.getName(), fn.manipulate(new AveragerFactoryWrapper(avg, avg.getName() + "_"), aggVal));
          } else {
            values.put(avg.getName(), null);
          }
        }

        return new MapBasedRow(result.getTimestamp(), values);

      }
    };

  }


  @Override
  public TypeReference<Row> getResultTypeReference()
  {
    return new TypeReference<Row>()
    {
    };
  }

  @Override
  public Function<Row, Row> makePreComputeManipulatorFn(RollingAverageQuery query, MetricManipulationFn fn)
  {
    return Functions.identity();
  }

}
