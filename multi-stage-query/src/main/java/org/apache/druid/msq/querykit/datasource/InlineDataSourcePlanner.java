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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.msq.input.inline.InlineInputSpec;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.DataSourcePlanner;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.spec.QuerySegmentSpec;

import java.util.Collections;

/**
 * Planner for {@link InlineDataSource}.
 */
public class InlineDataSourcePlanner implements DataSourcePlanner<InlineDataSource>
{
  @Override
  public DataSourcePlan planDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final InlineDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    DataSourcePlannerUtils.checkQuerySegmentSpecIsEternity(dataSource, querySegmentSpec);

    return new DataSourcePlan(
        dataSource,
        Collections.singletonList(new InlineInputSpec(dataSource)),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        null
    );
  }
}
