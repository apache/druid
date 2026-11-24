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
            join(JoinRelType.RIGHT, ImmutableList.of(RelHint.builder("broadcast").build()))
        )
    );
    Assertions.assertEquals(
        "RIGHT JOIN is not supported by engine[test] with join hint[broadcast]. "
        + "Remove the hint or use join hint[sort_merge].",
        e.getMessage()
    );
  }

  @Test
  public void test_rightJoin_broadcastHint_broadcastDefault_isRejected()
  {
    final ValidationException e = Assertions.assertThrows(
        ValidationException.class,
        () -> QueryValidations.validateLogicalQueryForDruid(
            plannerContext(JoinAlgorithm.BROADCAST),
            join(JoinRelType.RIGHT, ImmutableList.of(RelHint.builder("broadcast").build()))
        )
    );
    Assertions.assertEquals(
        "RIGHT JOIN is not supported by engine[test] with join hint[broadcast] and sqlJoinAlgorithm[broadcast]. "
        + "Remove the hint and set sqlJoinAlgorithm[sortMerge].",
        e.getMessage()
    );
  }

  @Test
  public void test_rightJoin_sortMergeHint_broadcastDefault_isRejected()
  {
    final ValidationException e = Assertions.assertThrows(
        ValidationException.class,
        () -> QueryValidations.validateLogicalQueryForDruid(
            plannerContext(JoinAlgorithm.BROADCAST),
            join(JoinRelType.RIGHT, ImmutableList.of(RelHint.builder("sort_merge").build()))
        )
    );
    Assertions.assertEquals(
        "RIGHT JOIN is not supported by engine[test] with sqlJoinAlgorithm[broadcast], "
        + "even with join hint[sort_merge]. Set sqlJoinAlgorithm[sortMerge].",
        e.getMessage()
    );
  }

  @Test
  public void test_rightJoin_noHint_sortMergeDefault_isAllowed() throws ValidationException
  {
    QueryValidations.validateLogicalQueryForDruid(
        plannerContext(JoinAlgorithm.SORT_MERGE),
        join(JoinRelType.RIGHT, ImmutableList.of())
    );
  }

  @Test
  public void test_rightJoin_broadcastDefault_isRejected()
  {
    final ValidationException e = Assertions.assertThrows(
        ValidationException.class,
        () -> QueryValidations.validateLogicalQueryForDruid(
            plannerContext(JoinAlgorithm.BROADCAST),
            join(JoinRelType.RIGHT, ImmutableList.of())
        )
    );
    Assertions.assertEquals(
        "RIGHT JOIN is not supported by engine[test] with sqlJoinAlgorithm[broadcast]. "
        + "Try sqlJoinAlgorithm[sortMerge].",
        e.getMessage()
    );
  }

  @Test
  public void test_fullJoin_broadcastHint_isRejected()
  {
    final ValidationException e = Assertions.assertThrows(
        ValidationException.class,
        () -> QueryValidations.validateLogicalQueryForDruid(
            plannerContext(JoinAlgorithm.SORT_MERGE),
            join(JoinRelType.FULL, ImmutableList.of(RelHint.builder("broadcast").build()))
        )
    );
    Assertions.assertEquals(
        "FULL JOIN is not supported by engine[test] with join hint[broadcast]. "
        + "Remove the hint or use join hint[sort_merge].",
        e.getMessage()
    );
  }

  @Test
  public void test_fullJoin_broadcastDefault_isRejected()
  {
    final ValidationException e = Assertions.assertThrows(
        ValidationException.class,
        () -> QueryValidations.validateLogicalQueryForDruid(
            plannerContext(JoinAlgorithm.BROADCAST),
            join(JoinRelType.FULL, ImmutableList.of())
        )
    );
    Assertions.assertEquals(
        "FULL JOIN is not supported by engine[test] with sqlJoinAlgorithm[broadcast]. "
        + "Try sqlJoinAlgorithm[sortMerge].",
        e.getMessage()
    );
  }

  @Test
  public void test_rightJoin_closestHintSortMerge_isAllowed() throws ValidationException
  {
    // The sort_merge hint has the shorter inherit path, so it overrides the inherited broadcast hint.
    QueryValidations.validateLogicalQueryForDruid(
        plannerContext(JoinAlgorithm.SORT_MERGE),
        join(
            JoinRelType.RIGHT,
            ImmutableList.of(
                RelHint.builder("broadcast").inheritPath(0, 0).build(),
                RelHint.builder("sort_merge").inheritPath(0).build()
            )
        )
    );
  }

  @Test
  public void test_rightJoin_closestHintBroadcast_isRejected()
  {
    // The broadcast hint has the shorter inherit path, so it overrides the inherited sort_merge hint.
    final ValidationException e = Assertions.assertThrows(
        ValidationException.class,
        () -> QueryValidations.validateLogicalQueryForDruid(
            plannerContext(JoinAlgorithm.SORT_MERGE),
            join(
                JoinRelType.RIGHT,
                ImmutableList.of(
                    RelHint.builder("sort_merge").inheritPath(0, 0).build(),
                    RelHint.builder("broadcast").inheritPath(0).build()
                )
            )
        )
    );
    Assertions.assertEquals(
        "RIGHT JOIN is not supported by engine[test] with join hint[broadcast]. "
        + "Remove the hint or use join hint[sort_merge].",
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
   * Join of {@code joinType} with {@code hints}, as the only node of a logical plan.
   */
  private static LogicalJoin join(final JoinRelType joinType, final ImmutableList<RelHint> hints)
  {
    final LogicalJoin join = Mockito.mock(LogicalJoin.class);
    Mockito.when(join.getJoinType()).thenReturn(joinType);
    Mockito.when(join.getHints()).thenReturn(hints);
    Mockito.when(join.accept(ArgumentMatchers.any(RelShuttle.class)))
           .thenAnswer(invocation -> invocation.<RelShuttle>getArgument(0).visit(join));
    return join;
  }
}
