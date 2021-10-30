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
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidRels;
import org.apache.druid.sql.calcite.rel.DruidUnionDataSourceRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Creates a {@link DruidUnionDataSourceRel} from various {@link DruidQueryRel} inputs that represent simple
 * table scans.
 */
public class DruidUnionDataSourceRule extends RelOptRule
{
  private static final DruidUnionDataSourceRule INSTANCE = new DruidUnionDataSourceRule();

  private DruidUnionDataSourceRule()
  {
    super(
        operand(
            Union.class,
            operand(DruidRel.class, none()),
            operand(DruidQueryRel.class, none())
        )
    );
  }

  public static DruidUnionDataSourceRule instance()
  {
    return INSTANCE;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final DruidRel<?> firstDruidRel = call.rel(1);
    final DruidQueryRel secondDruidRel = call.rel(2);

    return isCompatible(unionRel, firstDruidRel, secondDruidRel);
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
              getColumnNamesIfTableOrUnion(firstDruidRel).get(),
              firstDruidRel.getQueryMaker()
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
              getColumnNamesIfTableOrUnion(firstDruidRel).get(),
              firstDruidRel.getQueryMaker()
          )
      );
    }
  }

  // Can only do UNION ALL of inputs that have compatible schemas (or schema mappings) and right side
  // is a simple table scan
  public static boolean isCompatible(final Union unionRel, final DruidRel<?> first, final DruidRel<?> second)
  {
    if (!(second instanceof DruidQueryRel)) {
      return false;
    }

    return unionRel.all && isUnionCompatible(first, second);
  }

  private static boolean isUnionCompatible(final DruidRel<?> first, final DruidRel<?> second)
  {
    final Optional<List<String>> columnNames = getColumnNamesIfTableOrUnion(first);
    return columnNames.isPresent() && columnNames.equals(getColumnNamesIfTableOrUnion(second));
  }

  static Optional<List<String>> getColumnNamesIfTableOrUnion(final DruidRel<?> druidRel)
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
    } else {
      return Optional.empty();
    }
  }
}
