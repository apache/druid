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

import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.inline.InlineInputSpec;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LeafDataSource;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataSourcePlannerTest
{
  private static final QuerySegmentSpec ETERNITY =
      new MultipleIntervalSegmentSpec(List.of(Intervals.ETERNITY));

  private static final InputSpec PLANNED_SPEC =
      new InlineInputSpec(InlineDataSource.fromIterable(List.of(), RowSignature.empty()));

  @Test
  public void testUnhandledDataSourceThrowsWithNoPlanners()
  {
    Assertions.assertThrows(
        UOE.class,
        () -> plan(new TestDataSource(), Map.of())
    );
  }

  @Test
  public void testRegisteredPlannerHandlesItsDataSource()
  {
    final DataSourcePlan plan = plan(
        new TestDataSource(),
        Map.of(TestDataSource.class, new TestDataSourcePlanner<>())
    );

    Assertions.assertEquals(List.of(PLANNED_SPEC), plan.getInputSpecs());
  }

  @Test
  public void testPlannerDoesNotApplyToSubclassOfItsDataSource()
  {
    Assertions.assertThrows(
        UOE.class,
        () -> plan(new TestDataSourceSubclass(), Map.of(TestDataSource.class, new TestDataSourcePlanner<>()))
    );
  }

  @Test
  public void testRegisteredPlannerOverridesBuiltin()
  {
    final DataSourcePlan plan = plan(
        new TableDataSource("foo"),
        Map.of(TableDataSource.class, new TestDataSourcePlanner<>())
    );

    Assertions.assertEquals(List.of(PLANNED_SPEC), plan.getInputSpecs());
  }

  @Test
  public void testBuiltinPlannerHandlesTable()
  {
    final DataSourcePlan plan = plan(new TableDataSource("foo"), Map.of());

    Assertions.assertInstanceOf(TableInputSpec.class, Iterables.getOnlyElement(plan.getInputSpecs()));
  }

  @SuppressWarnings("rawtypes")
  private static DataSourcePlan plan(
      final DataSource dataSource,
      final Map<Class<? extends DataSource>, DataSourcePlanner> planners
  )
  {
    return DataSourcePlan.forDataSource(
        new QueryKitSpec(null, new DataSourcePlanners(planners), "queryId"),
        QueryContext.empty(),
        dataSource,
        ETERNITY,
        0,
        false
    );
  }

  private static class TestDataSource extends LeafDataSource
  {
    @Override
    public Set<String> getTableNames()
    {
      return Set.of();
    }

    @Override
    public boolean isCacheable(boolean isBroker)
    {
      return false;
    }

    @Override
    public boolean isGlobal()
    {
      return false;
    }

    @Override
    public boolean isProcessable()
    {
      return true;
    }

    @Override
    public byte[] getCacheKey()
    {
      return null;
    }
  }

  private static class TestDataSourceSubclass extends TestDataSource
  {
  }

  private static class TestDataSourcePlanner<T extends DataSource> implements DataSourcePlanner<T>
  {
    @Override
    public DataSourcePlan planDataSource(
        QueryKitSpec queryKitSpec,
        QueryContext queryContext,
        T dataSource,
        QuerySegmentSpec querySegmentSpec,
        int minStageNumber,
        boolean broadcast
    )
    {
      return new DataSourcePlan(dataSource, List.of(PLANNED_SPEC), IntSets.emptySet(), null);
    }
  }
}
