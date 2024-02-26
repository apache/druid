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

package org.apache.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.QueryContext;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.JoinAlgorithm;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DruidJoinRuleTest
{
  private final RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);

  private final RelDataType leftType =
      new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE).createStructType(
          ImmutableList.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
          ImmutableList.of("left")
      );

  private final RelDataType joinType =
      new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE).createStructType(
          ImmutableList.of(
              typeFactory.createSqlType(SqlTypeName.VARCHAR),
              typeFactory.createSqlType(SqlTypeName.VARCHAR)
          ),
          ImmutableList.of("left", "right")
      );

  private DruidJoinRule druidJoinRule;

  @BeforeEach
  void setup()
  {
    NullHandling.initializeForTests();
    PlannerContext plannerContext = Mockito.mock(PlannerContext.class);
    Mockito.when(plannerContext.queryContext()).thenReturn(QueryContext.empty());
    Mockito.when(plannerContext.getJoinAlgorithm()).thenReturn(JoinAlgorithm.BROADCAST);
    druidJoinRule = DruidJoinRule.instance(plannerContext);
  }

  @Test
  void can_handle_condition_left_eq_right()
  {
    assertTrue(
        druidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(joinType, 0),
                rexBuilder.makeInputRef(joinType, 1)
            ),
            leftType,
            null,
            JoinRelType.INNER,
            ImmutableList.of(),
            rexBuilder
        )
    );
  }

  @Test
  void can_handle_condition_left_fn_eq_right()
  {
    assertTrue(
        druidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.CONCAT,
                    rexBuilder.makeLiteral("foo"),
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0)
                ),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
            ),
            leftType,
            null,
            JoinRelType.INNER,
            ImmutableList.of(),
            rexBuilder
        )
    );
  }

  @Test
  void can_handle_condition_left_eq_right_fn()
  {
    assertEquals(
        NullHandling.sqlCompatible(), // We don't handle non-equi join conditions for non-sql compatible mode.
        druidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
                rexBuilder.makeCall(
                    SqlStdOperatorTable.CONCAT,
                    rexBuilder.makeLiteral("foo"),
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
                )
            ),
            leftType,
            null,
            JoinRelType.INNER,
            ImmutableList.of(),
            rexBuilder
        )
    );
  }

  @Test
  void can_handle_condition_left_eq_left()
  {

    assertEquals(
        NullHandling.sqlCompatible(), // We don't handle non-equi join conditions for non-sql compatible mode.
        druidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0)
            ),
            leftType,
            null,
            JoinRelType.INNER,
            ImmutableList.of(),
            rexBuilder
        )
    );
  }

  @Test
  void can_handle_condition_right_eq_right()
  {
    assertEquals(
        NullHandling.sqlCompatible(), // We don't handle non-equi join conditions for non-sql compatible mode.
        druidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
            ),
            leftType,
            null,
            JoinRelType.INNER,
            ImmutableList.of(),
            rexBuilder
        )
    );
  }

  @Test
  void can_handle_condition_left_eq_right_fn_left_join()
  {
    assertFalse(
        druidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
                rexBuilder.makeCall(
                    SqlStdOperatorTable.CONCAT,
                    rexBuilder.makeLiteral("foo"),
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
                )
            ),
            leftType,
            null,
            JoinRelType.LEFT,
            ImmutableList.of(),
            rexBuilder
        )
    );
  }

  @Test
  void can_handle_condition_left_eq_right_fn_system_fields()
  {
    assertFalse(
        druidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
                rexBuilder.makeCall(
                    SqlStdOperatorTable.CONCAT,
                    rexBuilder.makeLiteral("foo"),
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
                )
            ),
            leftType,
            null,
            JoinRelType.INNER,
            Collections.singletonList(null),
            rexBuilder
        )
    );
  }

  @Test
  void can_handle_condition_true()
  {
    assertTrue(
        druidJoinRule.canHandleCondition(
            rexBuilder.makeLiteral(true),
            leftType,
            null,
            JoinRelType.INNER,
            ImmutableList.of(),
            rexBuilder
        )
    );
  }

  @Test
  void can_handle_condition_false()
  {
    assertTrue(
        druidJoinRule.canHandleCondition(
            rexBuilder.makeLiteral(false),
            leftType,
            null,
            JoinRelType.INNER,
            ImmutableList.of(),
            rexBuilder
        )
    );
  }

  @Test
  void decompose_and_not_an_and()
  {
    final List<RexNode> rexNodes = DruidJoinRule.decomposeAnd(rexBuilder.makeInputRef(leftType, 0));

    assertEquals(1, rexNodes.size());
    assertEquals(rexBuilder.makeInputRef(leftType, 0), Iterables.getOnlyElement(rexNodes));
  }

  @Test
  void decompose_and_basic()
  {
    final List<RexNode> decomposed = DruidJoinRule.decomposeAnd(
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)),
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(2))
            ),
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(3)),
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(4))
            )
        )
    );

    assertEquals(
        ImmutableList.of(
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(2)),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(3)),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(4))
        ),
        decomposed
    );
  }
}
