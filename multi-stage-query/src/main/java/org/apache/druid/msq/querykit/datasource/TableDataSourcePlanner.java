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
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.DataSourcePlanner;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.joda.time.Interval;

import java.util.List;

/**
 * Planner for {@link TableDataSource}.
 */
public class TableDataSourcePlanner implements DataSourcePlanner<TableDataSource>
{
  @Override
  public DataSourcePlan planDataSource(
      final QueryKitSpec queryKitSpec,
      final QueryContext queryContext,
      final TableDataSource dataSource,
      final QuerySegmentSpec querySegmentSpec,
      final int minStageNumber,
      final boolean broadcast
  )
  {
    final List<SegmentDescriptor> segments;
    if (querySegmentSpec instanceof MultipleSpecificSegmentSpec) {
      segments = ((MultipleSpecificSegmentSpec) querySegmentSpec).getDescriptors();
    } else if (querySegmentSpec instanceof SpecificSegmentSpec) {
      segments = List.of(((SpecificSegmentSpec) querySegmentSpec).getDescriptor());
    } else {
      segments = null;
    }
    final List<Interval> intervals = querySegmentSpec.getIntervals();
    return new DataSourcePlan(
        (broadcast && dataSource.isGlobal()) ? dataSource : new InputNumberDataSource(0),
        List.of(new TableInputSpec(dataSource.getName(), intervals, segments)),
        broadcast ? IntOpenHashSet.of(0) : IntSets.emptySet(),
        null
    );
  }
}
