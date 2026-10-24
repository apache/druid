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

import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.rel.logical.DruidFilter;
import org.apache.druid.sql.calcite.rel.logical.DruidProject;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.apache.druid.sql.calcite.schema.SystemServerPropertiesTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

public class DruidBindableTableScanRuleTest
{
  private RelOptCluster cluster;
  private RelDataType rowType;
  private DruidBindableTableScanRule rule;

  @BeforeEach
  public void setUp()
  {
    final RexBuilder rexBuilder = new RexBuilder(DruidTypeSystem.TYPE_FACTORY);
    final VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    cluster = RelOptCluster.create(planner, rexBuilder);
    rowType = DruidTypeSystem.TYPE_FACTORY.builder()
                                          .add("server", SqlTypeName.VARCHAR)
                                          .add("service_name", SqlTypeName.VARCHAR)
                                          .add("node_roles", SqlTypeName.VARCHAR)
                                          .add("property", SqlTypeName.VARCHAR)
                                          .add("value", SqlTypeName.VARCHAR)
                                          .add("error_message", SqlTypeName.VARCHAR)
                                          .build();
    rule = new DruidBindableTableScanRule();
  }

  @Test
  public void testMatchesNativeSystemTableOnly()
  {
    final Bindables.BindableTableScan nativeScan = createScan(
        new SystemServerPropertiesTable(null, null, null, null, null),
        List.of(),
        identityProjects()
    );
    final Bindables.BindableTableScan nonNativeScan = createScan(
        Mockito.mock(ProjectableFilterableTable.class),
        List.of(),
        identityProjects()
    );
    final RelOptRuleCall call = Mockito.mock(RelOptRuleCall.class);

    Mockito.when(call.<Bindables.BindableTableScan>rel(0)).thenReturn(nativeScan, nonNativeScan);

    Assertions.assertTrue(rule.matches(call));
    Assertions.assertFalse(rule.matches(call));
  }

  @Test
  public void testConvertPreservesFilter()
  {
    final RexNode filter = equalsLiteral(3, "some.property");
    final Bindables.BindableTableScan bindableScan = createScan(
        new SystemServerPropertiesTable(null, null, null, null, null),
        List.of(filter),
        identityProjects()
    );

    final DruidFilter converted = Assertions.assertInstanceOf(DruidFilter.class, rule.convert(bindableScan));
    Assertions.assertEquals(filter, converted.getCondition());
    Assertions.assertInstanceOf(DruidTableScan.class, converted.getInput());
  }

  @Test
  public void testConvertPreservesFilterAndProjection()
  {
    final RexNode filter = equalsLiteral(3, "some.property");
    final Bindables.BindableTableScan bindableScan = createScan(
        new SystemServerPropertiesTable(null, null, null, null, null),
        List.of(filter),
        List.of(3, 1)
    );

    final DruidProject converted = Assertions.assertInstanceOf(DruidProject.class, rule.convert(bindableScan));
    final DruidFilter convertedFilter = Assertions.assertInstanceOf(DruidFilter.class, converted.getInput());
    Assertions.assertEquals(filter, convertedFilter.getCondition());
    Assertions.assertInstanceOf(DruidTableScan.class, convertedFilter.getInput());
    Assertions.assertEquals(bindableScan.getRowType(), converted.getRowType());
    Assertions.assertEquals(
        List.of(3, 1),
        converted.getProjects().stream().map(project -> ((RexInputRef) project).getIndex()).toList()
    );
  }

  private Bindables.BindableTableScan createScan(
      final Table table,
      final List<RexNode> filters,
      final List<Integer> projects
  )
  {
    return Bindables.BindableTableScan.create(cluster, createRelOptTable(table), filters, projects);
  }

  private RelOptTable createRelOptTable(final Table table)
  {
    final RelOptTable relOptTable = Mockito.mock(RelOptTable.class);
    Mockito.when(relOptTable.getRowType()).thenReturn(rowType);
    Mockito.when(relOptTable.getQualifiedName()).thenReturn(List.of("sys", "test"));
    Mockito.when(relOptTable.maybeUnwrap(ProjectableFilterableTable.class)).thenReturn(
        Optional.of((ProjectableFilterableTable) table)
    );
    Mockito.when(relOptTable.unwrap(Mockito.any())).thenAnswer(
        invocation -> {
          final Class<?> requestedClass = invocation.getArgument(0);
          return requestedClass.isInstance(table) ? table : null;
        }
    );
    return relOptTable;
  }

  private RexNode equalsLiteral(final int column, final String value)
  {
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    return rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(rowType.getFieldList().get(column).getType(), column),
        rexBuilder.makeLiteral(value)
    );
  }

  private List<Integer> identityProjects()
  {
    return List.of(0, 1, 2, 3, 4, 5);
  }
}
