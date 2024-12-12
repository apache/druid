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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidRels;
import org.apache.druid.sql.calcite.rel.DruidUnionDataSourceRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.table.DruidTable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Creates a {@link DruidUnionDataSourceRel} from various {@link DruidQueryRel} inputs that represent simple
 * table scans.
 */
public class DruidUnionDataSourceRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public DruidUnionDataSourceRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            Union.class,
            operand(DruidRel.class, none()),
            operand(DruidQueryRel.class, none())
        )
    );
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final DruidRel<?> firstDruidRel = call.rel(1);
    final DruidQueryRel secondDruidRel = call.rel(2);

    return isCompatible(unionRel, firstDruidRel, secondDruidRel, plannerContext);
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final DruidRel<?> firstDruidRel = call.rel(1);
    final DruidQueryRel secondDruidRel = call.rel(2);

    if (firstDruidRel instanceof DruidUnionDataSourceRel) {
      // Unwrap and flatten the inputs to the Union.
      final RelNode newUnionRel = call.builder()
                                      .pushAll(firstDruidRel.getInputs())
                                      .push(secondDruidRel)
                                      .union(true, firstDruidRel.getInputs().size() + 1)
                                      .build();

      call.transformTo(
          DruidUnionDataSourceRel.create(
              (Union) newUnionRel,
              getColumnNamesIfTableOrUnion(firstDruidRel, plannerContext).get(),
              firstDruidRel.getPlannerContext()
          )
      );
    } else {
      // Sanity check.
      if (!(firstDruidRel instanceof DruidQueryRel)) {
        throw new ISE("Expected first rel to be a DruidQueryRel, but it was %s", firstDruidRel.getClass().getName());
      }

      call.transformTo(
          DruidUnionDataSourceRel.create(
              unionRel,
              getColumnNamesIfTableOrUnion(firstDruidRel, plannerContext).get(),
              firstDruidRel.getPlannerContext()
          )
      );
    }
  }

  // Can only do UNION ALL of inputs that have compatible schemas (or schema mappings) and right side
  // is a simple table scan
  public static boolean isCompatible(
      final Union unionRel,
      final DruidRel<?> first,
      final DruidRel<?> second,
      @Nullable PlannerContext plannerContext
  )
  {
    if (!(second instanceof DruidQueryRel)) {
      return false;
    }

    if (!unionRel.all && null != plannerContext) {
      plannerContext.setPlanningError("SQL requires 'UNION' but only 'UNION ALL' is supported.");
    }
    return unionRel.all && isUnionCompatible(first, second, plannerContext);
  }

  private static boolean isUnionCompatible(final DruidRel<?> first, final DruidRel<?> second, @Nullable PlannerContext plannerContext)
  {
    final Optional<List<String>> firstColumnNames = getColumnNamesIfTableOrUnion(first, plannerContext);
    final Optional<List<String>> secondColumnNames = getColumnNamesIfTableOrUnion(second, plannerContext);
    if (!firstColumnNames.isPresent() || !secondColumnNames.isPresent()) {
      // No need to set the planning error here
      return false;
    }
    if (!firstColumnNames.equals(secondColumnNames)) {
      if (null != plannerContext) {
        plannerContext.setPlanningError("SQL requires union between two tables and column names queried for " +
            "each table are different Left: %s, Right: %s.",
            firstColumnNames.orElse(Collections.emptyList()),
            secondColumnNames.orElse(Collections.emptyList()));
      }
      return false;
    }
    return true;
  }

  static Optional<List<String>> getColumnNamesIfTableOrUnion(final DruidRel<?> druidRel, @Nullable PlannerContext plannerContext)
  {
    final PartialDruidQuery partialQuery = druidRel.getPartialDruidQuery();

    final Optional<DruidTable> druidTable =
        DruidRels.druidTableIfLeafRel(druidRel)
                 .filter(table -> table.getDataSource() instanceof TableDataSource);

    if (druidTable.isPresent() && DruidRels.isScanOrMapping(druidRel, false)) {
      // This rel is a table scan or mapping.

      if (partialQuery.stage() == PartialDruidQuery.Stage.SCAN) {
        return Optional.of(druidTable.get().getRowSignature().getColumnNames());
      } else {
        // Sanity check. Expected to be true due to the "scan or mapping" check.
        if (partialQuery.stage() != PartialDruidQuery.Stage.SELECT_PROJECT) {
          throw new ISE("Expected stage %s but got %s", PartialDruidQuery.Stage.SELECT_PROJECT, partialQuery.stage());
        }

        // Apply the mapping (with additional sanity checks).
        final RowSignature tableSignature = druidTable.get().getRowSignature();
        final Mappings.TargetMapping mapping = partialQuery.getSelectProject().getMapping();

        if (mapping.getSourceCount() != tableSignature.size()) {
          throw new ISE(
              "Expected mapping with %d columns but got %d columns",
              tableSignature.size(),
              mapping.getSourceCount()
          );
        }

        final List<String> retVal = new ArrayList<>();

        for (int i = 0; i < mapping.getTargetCount(); i++) {
          final int sourceField = mapping.getSourceOpt(i);
          retVal.add(tableSignature.getColumnName(sourceField));
        }

        return Optional.of(retVal);
      }
    } else if (!druidTable.isPresent() && druidRel instanceof DruidUnionDataSourceRel) {
      // This rel is a union itself.

      return Optional.of(((DruidUnionDataSourceRel) druidRel).getUnionColumnNames());
    } else if (druidTable.isPresent()) {
      if (null != plannerContext) {
        plannerContext.setPlanningError("SQL requires union between inputs that are not simple table scans " +
            "and involve a filter or aliasing. Or column types of tables being unioned are not of same type.");
      }
      return Optional.empty();
    } else {
      if (null != plannerContext) {
        plannerContext.setPlanningError("SQL requires union with input of a datasource type that is not supported."
            + " Union operation is only supported between regular tables. ");
      }
      return Optional.empty();
    }
  }
}
