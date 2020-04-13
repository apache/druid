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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
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

  @Test
  public void test_canHandleCondition_leftEqRight()
  {
    Assert.assertTrue(
        DruidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(joinType, 0),
                rexBuilder.makeInputRef(joinType, 1)
            ),
            leftType
        )
    );
  }

  @Test
  public void test_canHandleCondition_leftFnEqRight()
  {
    Assert.assertTrue(
        DruidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.CONCAT,
                    rexBuilder.makeLiteral("foo"),
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0)
                ),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
            ),
            leftType
        )
    );
  }

  @Test
  public void test_canHandleCondition_leftEqRightFn()
  {
    Assert.assertFalse(
        DruidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
                rexBuilder.makeCall(
                    SqlStdOperatorTable.CONCAT,
                    rexBuilder.makeLiteral("foo"),
                    rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
                )
            ),
            leftType
        )
    );
  }

  @Test
  public void test_canHandleCondition_leftEqLeft()
  {
    Assert.assertFalse(
        DruidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0)
            ),
            leftType
        )
    );
  }

  @Test
  public void test_canHandleCondition_rightEqRight()
  {
    Assert.assertFalse(
        DruidJoinRule.canHandleCondition(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)
            ),
            leftType
        )
    );
  }

  @Test
  public void test_canHandleCondition_true()
  {
    Assert.assertTrue(
        DruidJoinRule.canHandleCondition(
            rexBuilder.makeLiteral(true),
            leftType
        )
    );
  }

  @Test
  public void test_canHandleCondition_false()
  {
    Assert.assertTrue(
        DruidJoinRule.canHandleCondition(
            rexBuilder.makeLiteral(false),
            leftType
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
