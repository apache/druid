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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.QueryMaker;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.List;
import java.util.stream.Collectors;

public class DruidLogicalValuesRule extends RelOptRule
{
  private final QueryMaker queryMaker;

  public DruidLogicalValuesRule(QueryMaker queryMaker)
  {
    super(operand(LogicalValues.class, any()));
    this.queryMaker = queryMaker;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final LogicalValues values = call.rel(0);
    final List<ImmutableList<RexLiteral>> tuples = values.getTuples();
    final List<Object[]> objectTuples = tuples
        .stream()
        .map(tuple -> tuple.stream().map(this::getValueFromLiteral).collect(Collectors.toList()).toArray(new Object[0]))
        .collect(Collectors.toList());
    final RowSignature rowSignature = RowSignatures.fromRelDataType(
        values.getRowType().getFieldNames(),
        values.getRowType()
    );
    final DruidTable druidTable = new DruidTable(
        InlineDataSource.fromIterable(objectTuples, rowSignature),
        rowSignature,
        true,
        false
    );
    call.transformTo(
        DruidQueryRel.fullScan(values, druidTable, queryMaker)
    );
  }

  /**
   * Similar to {@link RexLiteral#getValue2()} but takes time zone into account when retrieving date/time types.
   */
  private Object getValueFromLiteral(RexLiteral literal)
  {
    switch (literal.getTypeName()) {
      case CHAR:
        return literal.getValueAs(String.class);
      case DECIMAL:
        return literal.getValueAs(Long.class);
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DATE:
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
        return Calcites.calciteDateTimeLiteralToJoda(literal, queryMaker.getPlannerContext().getTimeZone()).getMillis();
      default:
        return literal.getValue2();
    }
  }
}
