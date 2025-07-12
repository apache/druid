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

package org.apache.druid.msq.test;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.CalciteJoinQueryTest;
import org.apache.druid.sql.calcite.NotYetSupported;
import org.apache.druid.sql.calcite.NotYetSupported.Modes;
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.junit.AssumptionViolatedException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@SqlTestFrameworkConfig.ComponentSupplier(DartComponentSupplier.class)
public abstract class DecoupledDartCalciteJoinQueryTest extends CalciteJoinQueryTest
{

  @Nested
  public static class BroadcastTest extends DecoupledDartCalciteJoinQueryTest
  {
    @Override
    protected JoinAlgorithm joinAlgorithm()
    {
      return JoinAlgorithm.BROADCAST;
    }
  }

  @Nested
  public static class SortMergeTest extends DecoupledDartCalciteJoinQueryTest
  {
    @Override
    protected JoinAlgorithm joinAlgorithm()
    {
      return JoinAlgorithm.SORT_MERGE;
    }

    @NotYetSupported(Modes.DD_JOIN_CONDITION_NORMALIZATION)
    @Test
    @Override
    public void testJoinWithInputRefCondition()
    {
      super.testJoinWithInputRefCondition();
    }
  }

  @RegisterExtension
  NotYetSupportedProcessor notYetSupportedProcessor = new NotYetSupportedProcessor(
      NotYetSupported.Scope.DECOUPLED_DART
  );

  @RegisterExtension
  DecoupledDartExtension decoupledExtension = new DecoupledDartExtension(this);

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return decoupledExtension.testBuilder()
        .queryContext(
            ImmutableMap.<String, Object>builder()
                .put(QueryContexts.CTX_PREPLANNED, true)
                .put(
                    QueryContexts.CTX_NATIVE_QUERY_SQL_PLANNING_MODE,
                    QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
                )
                .put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, decoupledExtension)
                .put(PlannerContext.CTX_SQL_JOIN_ALGORITHM, joinAlgorithm().toString())
                .put(QueryContexts.ENABLE_DEBUG, true)
                .build()
        );
  }

  protected abstract JoinAlgorithm joinAlgorithm();

  @Override
  protected void cannotVectorize()
  {
  }

  @Override
  protected void cannotVectorizeUnlessFallback()
  {
  }

  @Override
  protected void msqIncompatible()
  {
    throw new AssumptionViolatedException("Case marked as msqIncompatible; not trying dart right now");
  }

  @Override
  public boolean isSortBasedJoin()
  {
    return joinAlgorithm() == JoinAlgorithm.SORT_MERGE;
  }

  @Override
  protected boolean isRunningMSQ()
  {
    return true;
  }
}
