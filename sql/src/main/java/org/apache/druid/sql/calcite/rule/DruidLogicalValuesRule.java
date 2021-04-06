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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.QueryMaker;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link RelOptRule} that converts {@link LogicalValues} into {@link InlineDataSource}.
 * This rule is used when the query directly reads in-memory tuples. For example, given a query of
 * `SELECT 1 + 1`, the query planner will create {@link LogicalValues} that contains one tuple,
 * which in turn containing one column of value 2.
 *
 * The query planner can sometimes reduce a regular query to a query that reads in-memory tuples.
 * For example, `SELECT count(*) FROM foo WHERE 1 = 0` is reduced to `SELECT 0`. This rule will
 * be used for this case as well.
 */
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
        .map(tuple -> tuple
            .stream()
            .map(v -> getValueFromLiteral(v, queryMaker.getPlannerContext()))
            .collect(Collectors.toList())
            .toArray(new Object[0])
        )
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
   * Retrieves value from the literal based on Druid data type mapping
   * (https://druid.apache.org/docs/latest/querying/sql.html#standard-types).
   *
   * @throws IllegalArgumentException for unsupported types
   */
  @VisibleForTesting
  static Object getValueFromLiteral(RexLiteral literal, PlannerContext plannerContext)
  {
    switch (literal.getType().getSqlTypeName()) {
      case CHAR:
      case VARCHAR:
        return literal.getValueAs(String.class);
      case FLOAT:
        return literal.getValueAs(Float.class);
      case DOUBLE:
      case REAL:
      case DECIMAL:
        return literal.getValueAs(Double.class);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return literal.getValueAs(Long.class);
      case BOOLEAN:
        return literal.isAlwaysTrue() ? 1L : 0L;
      case TIMESTAMP:
      case DATE:
        return Calcites.calciteDateTimeLiteralToJoda(literal, plannerContext.getTimeZone()).getMillis();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      default:
        throw new IAE("Unsupported type[%s]", literal.getTypeName());
    }
  }
}
