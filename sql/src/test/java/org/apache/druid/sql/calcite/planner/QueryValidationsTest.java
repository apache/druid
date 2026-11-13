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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class QueryValidationsTest
{
  @Test
  public void test_rightJoin_broadcastHint_isRejected()
  {
    final ValidationException e = Assertions.assertThrows(
        ValidationException.class,
        () -> QueryValidations.validateLogicalQueryForDruid(
            plannerContext(JoinAlgorithm.SORT_MERGE),
            rightJoin(ImmutableList.of(RelHint.builder("broadcast").build()))
        )
    );
    Assertions.assertEquals(
        "RIGHT JOIN is not supported by engine[test] with join hint[broadcast]. Remove the hint.",
        e.getMessage()
    );
  }

  @Test
  public void test_rightJoin_noHint_sortMergeDefault_isAllowed() throws ValidationException
  {
    QueryValidations.validateLogicalQueryForDruid(
        plannerContext(JoinAlgorithm.SORT_MERGE),
        rightJoin(ImmutableList.of())
    );
  }

  @Test
  public void test_rightJoin_broadcastDefault_isRejected()
  {
    final ValidationException e = Assertions.assertThrows(
        ValidationException.class,
        () -> QueryValidations.validateLogicalQueryForDruid(
            plannerContext(JoinAlgorithm.BROADCAST),
            rightJoin(ImmutableList.of())
        )
    );
    Assertions.assertEquals(
        "RIGHT JOIN is not supported by engine[test] with sqlJoinAlgorithm[broadcast]. "
        + "Try sqlJoinAlgorithm[sortMerge].",
        e.getMessage()
    );
  }

  /**
   * Planner context for an engine that cannot run RIGHT or FULL broadcast joins.
   */
  private static PlannerContext plannerContext(final JoinAlgorithm defaultJoinAlgorithm)
  {
    final SqlEngine engine = Mockito.mock(SqlEngine.class);
    Mockito.when(engine.name()).thenReturn("test");

    final PlannerContext plannerContext = Mockito.mock(PlannerContext.class);
    Mockito.when(plannerContext.featureAvailable(EngineFeature.ALLOW_BROADCAST_RIGHTY_JOIN)).thenReturn(false);
    Mockito.when(plannerContext.getJoinAlgorithm()).thenReturn(defaultJoinAlgorithm);
    Mockito.when(plannerContext.getEngine()).thenReturn(engine);
    return plannerContext;
  }

  /**
   * RIGHT join with {@code hints}, as the only node of a logical plan.
   */
  private static LogicalJoin rightJoin(final ImmutableList<RelHint> hints)
  {
    final LogicalJoin join = Mockito.mock(LogicalJoin.class);
    Mockito.when(join.getJoinType()).thenReturn(JoinRelType.RIGHT);
    Mockito.when(join.getHints()).thenReturn(hints);
    Mockito.when(join.accept(ArgumentMatchers.any(RelShuttle.class)))
           .thenAnswer(invocation -> invocation.<RelShuttle>getArgument(0).visit(join));
    return join;
  }
}
