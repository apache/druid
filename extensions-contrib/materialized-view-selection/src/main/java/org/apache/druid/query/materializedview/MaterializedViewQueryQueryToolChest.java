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

package org.apache.druid.query.materializedview;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.context.ResponseContext;

public class MaterializedViewQueryQueryToolChest extends QueryToolChest 
{
  private final QueryToolChestWarehouse warehouse;
  private DataSourceOptimizer optimizer;

  @Inject
  public MaterializedViewQueryQueryToolChest(
      QueryToolChestWarehouse warehouse
  )
  {
    this.warehouse = warehouse;
  }
  
  @Override
  public QueryRunner mergeResults(QueryRunner runner)
  {
    return new QueryRunner() {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
      {
        Query realQuery = getRealQuery(queryPlus.getQuery());
        return warehouse.getToolChest(realQuery).mergeResults(runner).run(queryPlus.withQuery(realQuery), responseContext);
      }
    };
  }

  @Override
  public QueryMetrics makeMetrics(Query query) 
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makeMetrics(realQuery);
  }
  
  @Override
  public Function makePreComputeManipulatorFn(Query query, MetricManipulationFn fn) 
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makePreComputeManipulatorFn(realQuery, fn);
  }

  @Override
  public Function makePostComputeManipulatorFn(Query query, MetricManipulationFn fn)
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makePostComputeManipulatorFn(realQuery, fn);
  }

  @Override
  public TypeReference getResultTypeReference()
  {
    return null;
  }

  @Override
  public QueryRunner preMergeQueryDecoration(final QueryRunner runner)
  {
    return new QueryRunner() {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
      {
        Query realQuery = getRealQuery(queryPlus.getQuery());
        QueryToolChest realQueryToolChest = warehouse.getToolChest(realQuery);
        QueryRunner realQueryRunner = realQueryToolChest.preMergeQueryDecoration(
            new MaterializedViewQueryRunner(runner, optimizer)
        );
        return realQueryRunner.run(queryPlus.withQuery(realQuery), responseContext);
      }
    };
  }
  
  public Query getRealQuery(Query query)
  {
    if (query instanceof MaterializedViewQuery) {
      optimizer = ((MaterializedViewQuery) query).getOptimizer();
      return ((MaterializedViewQuery) query).getQuery();
    }
    return query;
  }
}
