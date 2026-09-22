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

package org.apache.druid.msq.querykit;

import com.google.inject.Binder;
import org.apache.druid.msq.guice.MSQBinders;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.spec.QuerySegmentSpec;

/**
 * Builds a {@link DataSourcePlan} for a particular class of {@link DataSource}. Register with
 * {@link MSQBinders#dataSourcePlannerBinder(Binder)}.
 */
public interface DataSourcePlanner<T extends DataSource>
{
  /**
   * Same contract as {@link DataSourcePlan#forDataSource}, for the one datasource type this planner is registered
   * against.
   *
   * @param queryKitSpec     reference for recursive planning
   * @param queryContext     query context
   * @param dataSource       datasource to plan
   * @param querySegmentSpec intervals for mandatory pruning. The returned plan must be filtered to this interval.
   * @param minStageNumber   starting stage number for subqueries
   * @param broadcast        whether the plan should broadcast data for this datasource
   */
  DataSourcePlan planDataSource(
      QueryKitSpec queryKitSpec,
      QueryContext queryContext,
      T dataSource,
      QuerySegmentSpec querySegmentSpec,
      int minStageNumber,
      boolean broadcast
  );
}
