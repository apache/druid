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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.NotYetSupported.Modes;
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@SqlTestFrameworkConfig.ComponentSupplier(CalciteSysQueryTest.BindableComponentSupplier.class)
public class CalciteSysQueryTest extends BaseCalciteQueryTest
{
  public static class BindableComponentSupplier extends StandardComponentSupplier
  {
    public BindableComponentSupplier(final TempDirProducer tempDirProducer)
    {
      super(tempDirProducer);
    }

    @Override
    public Class<? extends SqlEngine> getSqlEngineClass()
    {
      return BindableTestSqlEngine.class;
    }
  }

  public static class BindableTestSqlEngine extends NativeSqlEngine
  {
    @Inject
    public BindableTestSqlEngine(
        final QueryLifecycleFactory queryLifecycleFactory,
        final ObjectMapper jsonMapper,
        final SqlToolbox toolbox
    )
    {
      super(queryLifecycleFactory, jsonMapper, toolbox);
    }

    @Override
    public String name()
    {
      return "bindable-test";
    }
  }

  @RegisterExtension
  NotYetSupportedProcessor notYetSupportedProcessor = new NotYetSupportedProcessor(NotYetSupported.Scope.BINDABLE);

  @Test
  public void testTasksSum()
  {
    msqIncompatible();

    testBuilder()
        .sql("select datasource, sum(duration) from sys.tasks group by datasource")
        .expectedResults(ImmutableList.of(
            new Object[]{"foo", 11L},
            new Object[]{"foo2", 22L}
        ))
        .expectedLogicalPlan("LogicalAggregate(group=[{0}], EXPR$1=[SUM($1)])\n"
                             + "  LogicalProject(exprs=[[$3, $8]])\n"
                             + "    LogicalTableScan(table=[[sys, tasks]])\n")
        .run();
  }

  @NotYetSupported(Modes.EXPRESSION_NOT_GROUPED)
  @Test
  public void testTasksSumOver()
  {
    msqIncompatible();

    testBuilder()
        .sql("select datasource, sum(duration) over () from sys.tasks group by datasource")
        .expectedResults(ImmutableList.of(
            new Object[]{"foo", 11L},
            new Object[]{"foo2", 22L}
        ))
        // please add expectedLogicalPlan if this test starts passing!
        .run();
  }

  @Test
  public void testRoundOnSysTableColumn()
  {
    msqIncompatible();

    testBuilder()
        .sql("select round(duration, 1) from sys.tasks ")
        .expectedResults(ImmutableList.of(
            new Object[]{10L},
            new Object[]{1L},
            new Object[]{20L},
            new Object[]{2L}
        ))
        .expectedLogicalPlan("LogicalProject(exprs=[[ROUND($8, 1)]])\n"
                             + "  LogicalTableScan(table=[[sys, tasks]])\n")
        .run();
  }

  @Test
  public void testRoundOnAvgOnSysTableColumn()
  {
    msqIncompatible();

    testBuilder()
        .sql("select round(avg(duration), 1) from sys.tasks ")
        .expectedResults(ImmutableList.of(
            new Object[]{8.3D}))
        .expectedLogicalPlan("LogicalProject(exprs=[[ROUND($0, 1)]])\n"
                             + "  LogicalAggregate(group=[{}], agg#0=[AVG($0)])\n"
                             + "    LogicalProject(exprs=[[$8]])\n"
                             + "      LogicalTableScan(table=[[sys, tasks]])\n")
        .run();
  }
}
