/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.materializedview;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.inject.Inject;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.aggregation.MetricManipulationFn;

import java.util.Map;

public class MaterializedViewQueryQueryToolChest extends QueryToolChest 
{
  private final QueryToolChestWarehouse warehouse;

  @Inject
  public MaterializedViewQueryQueryToolChest(QueryToolChestWarehouse warehouse)
  {
    this.warehouse = warehouse;
  }
  
  @Override
  public QueryRunner mergeResults(QueryRunner runner)
  {
    return new QueryRunner() {
      @Override
      public Sequence run(QueryPlus queryPlus, Map responseContext) 
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
  public TypeReference getResultTypeReference()
  {
    return null;
  }

  @Override
  public QueryRunner preMergeQueryDecoration(final QueryRunner runner)
  {
    return new QueryRunner() {
      @Override
      public Sequence run(QueryPlus queryPlus, Map responseContext)
      {
        Query realQuery = getRealQuery(queryPlus.getQuery());
        QueryToolChest realQueryToolChest = warehouse.getToolChest(realQuery);
        QueryRunner realQueryRunner = realQueryToolChest.preMergeQueryDecoration(
            new MaterializedViewQueryRunner(runner)
        );
        return realQueryRunner.run(queryPlus.withQuery(realQuery), responseContext);
      }
    };
  }
  
  public Query getRealQuery(Query query)
  {
    if (query instanceof MaterializedViewQuery) {
      return ((MaterializedViewQuery) query).getRealQuery();
    }
    return query;
  }
}
