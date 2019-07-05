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

package org.apache.druid.sql.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.List;

public abstract class DruidRel<T extends DruidRel> extends AbstractRelNode
{
  private final QueryMaker queryMaker;

  protected DruidRel(RelOptCluster cluster, RelTraitSet traitSet, QueryMaker queryMaker)
  {
    super(cluster, traitSet);
    this.queryMaker = queryMaker;
  }

  /**
   * Returns the PartialDruidQuery associated with this DruidRel, and which can be built on top of. Returns null
   * if this rel cannot be built on top of.
   */
  @Nullable
  public abstract PartialDruidQuery getPartialDruidQuery();

  /**
   * Return the number of Druid queries this rel involves, including sub-queries. Simple queries will return 1.
   *
   * @return number of nested queries
   */
  public abstract int getQueryCount();

  public abstract Sequence<Object[]> runQuery();

  public abstract T withPartialQuery(PartialDruidQuery newQueryBuilder);

  public boolean isValidDruidQuery()
  {
    try {
      toDruidQueryForExplaining();
      return true;
    }
    catch (CannotBuildQueryException e) {
      return false;
    }
  }

  /**
   * Convert this DruidRel to a DruidQuery. This may be an expensive operation. For example, DruidSemiJoin needs to
   * execute the right-hand side query in order to complete this method.
   *
   * This method may return null if it knows that this rel will yield an empty result set.
   *
   * @param finalizeAggregations true if this query should include explicit finalization for all of its
   *                             aggregators, where required. Useful for subqueries where Druid's native query layer
   *                             does not do this automatically.
   *
   * @return query, or null if it is known in advance that this rel will yield an empty result set.
   *
   * @throws CannotBuildQueryException
   */
  @Nullable
  public abstract DruidQuery toDruidQuery(boolean finalizeAggregations);

  /**
   * Convert this DruidRel to a DruidQuery for purposes of explaining. This must be an inexpensive operation. For
   * example, DruidSemiJoin will use a dummy dataSource in order to complete this method, rather than executing
   * the right-hand side query.
   *
   * This method may not return null.
   *
   * @return query
   *
   * @throws CannotBuildQueryException
   */
  public abstract DruidQuery toDruidQueryForExplaining();

  public QueryMaker getQueryMaker()
  {
    return queryMaker;
  }

  public PlannerContext getPlannerContext()
  {
    return queryMaker.getPlannerContext();
  }

  public abstract T asDruidConvention();

  /**
   * Get a list of names of datasources read by this DruidRel
   */
  public abstract List<String> getDataSourceNames();
}
