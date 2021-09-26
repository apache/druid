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
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL;

public class FilterJoinExcludePushToChildRuleTest
{
  private final RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl());
  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);

  @Test
  public void testRemoveRedundantIsNotNullFiltersWithSQLCompatibility()
  {
    RexNode equalityFilter = rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0),
        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1));
    RexNode isNotNullFilterOnJoinColumn = rexBuilder.makeCall(IS_NOT_NULL,
                                                  ImmutableList.of(rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1)));
    RexNode isNotNullFilterOnNonJoinColumn = rexBuilder.makeCall(IS_NOT_NULL,
                                                  ImmutableList.of(rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2)));
    List<RexNode> joinFilters = new ArrayList<>();
    joinFilters.add(equalityFilter);

    FilterJoinExcludePushToChildRule.removeRedundantIsNotNullFilters(joinFilters, JoinRelType.INNER, true);
    Assert.assertEquals(joinFilters.size(), 1);
    Assert.assertEquals("Equality Filter changed", joinFilters.get(0), equalityFilter);

    // add IS NOT NULL filter on a join column
    joinFilters.add(isNotNullFilterOnNonJoinColumn);
    joinFilters.add(isNotNullFilterOnJoinColumn);
    Assert.assertEquals(joinFilters.size(), 3);
    FilterJoinExcludePushToChildRule.removeRedundantIsNotNullFilters(joinFilters, JoinRelType.INNER, true);
    Assert.assertEquals(joinFilters.size(), 2);
    Assert.assertEquals("Equality Filter changed", joinFilters.get(0), equalityFilter);
    Assert.assertEquals("IS NOT NULL filter on non-join column changed", joinFilters.get(1), isNotNullFilterOnNonJoinColumn);
  }
}
