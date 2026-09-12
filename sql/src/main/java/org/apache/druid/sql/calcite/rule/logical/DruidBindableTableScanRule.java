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

package org.apache.druid.sql.calcite.rule.logical;

import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.druid.sql.calcite.rel.logical.DruidFilter;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalConvention;
import org.apache.druid.sql.calcite.rel.logical.DruidProject;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.apache.druid.sql.calcite.schema.SystemSchema;

import java.util.List;
import java.util.stream.Collectors;

/// Converts a native system table that Calcite has represented as a [Bindables.BindableTableScan]. Calcite may push
/// filters and projections into this node for a `ProjectableFilterableTable`; they must be restored as Druid logical
/// nodes so native query generation can process them.
///
/// This rule is used only by the **DECOUPLED** planner. The coupled planner converts a regular table scan through
/// `DruidTableScanRule` before Calcite produces a `BindableTableScan`.
///
/// ```
/// BindableTableScan(filters, projects)
///   -> DruidProject
///        `- DruidFilter
///             `- DruidTableScan
/// ```
///
/// The project and filter nodes are omitted when the Bindable scan contains an identity projection or no filters.
public class DruidBindableTableScanRule extends ConverterRule
{
  public DruidBindableTableScanRule()
  {
    super(
        Config.INSTANCE.withConversion(
            Bindables.BindableTableScan.class,
            BindableConvention.INSTANCE,
            DruidLogicalConvention.instance(),
            DruidBindableTableScanRule.class.getSimpleName()
        )
    );
  }

  @Override
  public boolean matches(final RelOptRuleCall call)
  {
    final Bindables.BindableTableScan scan = call.rel(0);
    // QueryHandler has already selected native planning; only native-capable system tables can use this conversion.
    return SystemSchema.getNativeSystemTable(scan.getTable()) != null;
  }

  @Override
  public RelNode convert(final RelNode rel)
  {
    final Bindables.BindableTableScan bindableScan = (Bindables.BindableTableScan) rel;
    RelNode current = new DruidTableScan(
        bindableScan.getCluster(),
        bindableScan.getTraitSet().replace(DruidLogicalConvention.instance()),
        bindableScan.getTable()
    );

    if (!bindableScan.filters.isEmpty()) {
      current = new DruidFilter(
          bindableScan.getCluster(),
          current.getTraitSet(),
          current,
          RexUtil.composeConjunction(bindableScan.getCluster().getRexBuilder(), bindableScan.filters)
      );
    }

    if (!bindableScan.projects.equals(bindableScan.identity())) {
      final RexBuilder rexBuilder = bindableScan.getCluster().getRexBuilder();
      final RelNode projectInput = current;
      final List<RexNode> projects = bindableScan.projects
                                                    .stream()
                                                    .map(index -> rexBuilder.makeInputRef(projectInput, index))
                                                    .collect(Collectors.toList());
      current = DruidProject.create(current, projects, bindableScan.getRowType());
    }

    return current;
  }
}
