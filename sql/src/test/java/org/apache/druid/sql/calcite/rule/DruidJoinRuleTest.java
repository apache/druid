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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

public class DruidJoinRuleTest
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

  @Before
  public void setup()
  {
    NullHandling.initializeForTests();
    PlannerContext plannerContext = Mockito.mock(PlannerContext.class);
    Mockito.when(plannerContext.queryContext()).thenReturn(QueryContext.empty());
    Mockito.when(plannerContext.getJoinAlgorithm()).thenReturn(JoinAlgorithm.BROADCAST);
    druidJoinRule = DruidJoinRule.instance(plannerContext);
  }

  @Test
  public void test_canHandleCondition_leftEqRight()
  {
    Assert.assertTrue(
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
  public void test_canHandleCondition_leftFnEqRight()
  {
    Assert.assertTrue(
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
  public void test_canHandleCondition_leftEqRightFn()
  {
    Assert.assertEquals(
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
  public void test_canHandleCondition_leftEqLeft()
  {

    Assert.assertEquals(
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
  public void test_canHandleCondition_rightEqRight()
  {
    Assert.assertEquals(
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
  public void test_canHandleCondition_leftEqRightFn_leftJoin()
  {
    Assert.assertFalse(
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
  public void test_canHandleCondition_leftEqRightFn_systemFields()
  {
    Assert.assertFalse(
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
  public void test_canHandleCondition_true()
  {
    Assert.assertTrue(
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
  public void test_canHandleCondition_false()
  {
    Assert.assertTrue(
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
  public void test_decomposeAnd_notAnAnd()
  {
    final List<RexNode> rexNodes = DruidJoinRule.decomposeAnd(rexBuilder.makeInputRef(leftType, 0));

    Assert.assertEquals(1, rexNodes.size());
    Assert.assertEquals(rexBuilder.makeInputRef(leftType, 0), Iterables.getOnlyElement(rexNodes));
  }

  @Test
  public void test_decomposeAnd_basic()
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

    Assert.assertEquals(
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
