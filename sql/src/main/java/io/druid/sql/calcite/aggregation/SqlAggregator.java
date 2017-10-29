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

package io.druid.sql.calcite.aggregation;

import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Bridge between Druid and SQL aggregators.
 */
public interface SqlAggregator
{
  /**
   * Returns the SQL operator corresponding to this aggregation function. Should be a singleton.
   *
   * @return operator
   */
  SqlAggFunction calciteFunction();

  /**
   * Returns a Druid Aggregation corresponding to a SQL {@link AggregateCall}. This method should ignore filters;
   * they will be applied to your aggregator in a later step.
   *
   * @param plannerContext       SQL planner context
   * @param rowSignature         signature of the rows being aggregated
   * @param rexBuilder           a rexBuilder, in case you need one
   * @param name                 desired output name of the aggregation
   * @param aggregateCall        aggregate call object
   * @param project              project that should be applied before aggregation; may be null
   * @param existingAggregations existing aggregations for this query; useful for re-using aggregations. May be safely
   *                             ignored if you do not want to re-use existing aggregations.
   *
   * @return aggregation, or null if the call cannot be translated
   */
  @Nullable
  Aggregation toDruidAggregation(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexBuilder rexBuilder,
      String name,
      AggregateCall aggregateCall,
      Project project,
      List<Aggregation> existingAggregations
  );
}
