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

import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
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
   * Returns Druid Aggregation corresponding to a SQL {@link AggregateCall}.
   *
   * @param name                 desired output name of the aggregation
   * @param rowSignature         signature of the rows being aggregated
   * @param existingAggregations existing aggregations for this query; useful for re-using aggregators
   * @param project              SQL projection to apply before the aggregate call
   * @param aggregateCall        SQL aggregate call
   *
   * @return aggregation, or null if the call cannot be translated
   */
  @Nullable
  Aggregation toDruidAggregation(
      final String name,
      final RowSignature rowSignature,
      final List<Aggregation> existingAggregations,
      final Project project,
      final AggregateCall aggregateCall
  );
}
