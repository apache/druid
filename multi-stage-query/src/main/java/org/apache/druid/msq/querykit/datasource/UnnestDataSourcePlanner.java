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

package org.apache.druid.msq.querykit.datasource;

import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.DataSourcePlanner;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.spec.QuerySegmentSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner for {@link UnnestDataSource}. Plans the base datasource, then reapplies the unnest.
 */
public class UnnestDataSourcePlanner implements DataSourcePlanner<UnnestDataSource>
{
  @Override
  public DataSourcePlan planDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final UnnestDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    // Find the plan for base data source by recursing
    final DataSourcePlan basePlan = DataSourcePlan.forDataSource(
        queryKitSpec,
        queryContext,
        dataSource.getBase(),
        querySegmentSpec,
        minStageNumber,
        broadcast
    );

    final List<InputSpec> inputSpecs = new ArrayList<>(basePlan.getInputSpecs());

    // Create the new data source using the data source from the base plan
    final DataSource newDataSource = UnnestDataSource.create(
        basePlan.getNewDataSource(),
        dataSource.getVirtualColumn(),
        dataSource.getUnnestFilter()
    );

    // The base data source can be a join and might already have broadcast inputs
    // Need to set the broadcast inputs from the basePlan
    return new DataSourcePlan(
        newDataSource,
        inputSpecs,
        basePlan.getBroadcastInputs(),
        basePlan.getSubQueryDefBuilder().orElse(null)
    );
  }
}
