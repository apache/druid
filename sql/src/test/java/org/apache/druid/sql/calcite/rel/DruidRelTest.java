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

package org.apache.druid.sql.calcite.rel;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.sql.calcite.planner.ExpressionParser;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerToolbox;
import org.apache.druid.sql.calcite.rel.logical.DruidUnion;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;

import static org.apache.druid.sql.calcite.util.CalciteTests.POLICY_RESTRICTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class DruidRelTest
{
  private static final RowSignature ROW_SIGNATURE = RowSignature.builder().add("col1", ColumnType.LONG).build();
  private static final RelDataType LONG_TYPE = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);
  private static final RelDataTypeFieldImpl COL1_FIELD = new RelDataTypeFieldImpl("col1", 0, LONG_TYPE);
  private static final RelRecordType REC_TYPE = new RelRecordType(StructKind.NONE, ImmutableList.of(COL1_FIELD), true);
  private static final RelDataTypeFactory DEFAULT_TYPE_FACTORY = new JavaTypeFactoryImpl();
  private static final RexLiteral ALWAYS_TRUE = new RexBuilder(DEFAULT_TYPE_FACTORY).makeLiteral(true);
  private static final Expr EXPR_6L = ExprEval.ofLong(6).toExpr();
  private static final TableDataSource TABLE = TableDataSource.create("restricted_foo");
  private static final RestrictedDataSource RESTRICTED = RestrictedDataSource.create(TABLE, POLICY_RESTRICTION);
  private static final PhysicalDatasourceMetadata RESTRICTED_METADATA = new PhysicalDatasourceMetadata(
      TABLE,
      ROW_SIGNATURE,
      true,
      false
  );
  private static final DruidTable DRUID_TABLE = new DatasourceTable(RESTRICTED_METADATA);
  private static final ImmutableMap<String, Optional<Policy>> RESTRICTIONS = ImmutableMap.of(
      "restricted_foo",
      Optional.of(POLICY_RESTRICTION)
  );
  private static final AuthorizationResult AUTHORIZATION_RESULT = AuthorizationResult.allowWithRestriction(RESTRICTIONS);


  @Mock
  private RelOptTable mockRelOptTable;
  @Mock
  private RelOptCluster mockRelOptCluster;
  @Mock
  private RelTraitSet mockRelTraitSet;

  @Mock
  private PlannerContext mockPlannerContext;
  private DruidQueryRel druidQueryRelNode;

  @Before
  public void setup() throws Exception
  {
    MockitoAnnotations.openMocks(this);
    when(mockRelOptCluster.getTypeFactory()).thenReturn(DEFAULT_TYPE_FACTORY);
    when(mockRelOptTable.getRowType()).thenReturn(REC_TYPE);

    PlannerToolbox mockPlannerToolbox = mock(PlannerToolbox.class);
    when(mockPlannerToolbox.getPolicyEnforcer()).thenReturn(NoopPolicyEnforcer.instance());
    when(mockPlannerContext.getPlannerToolbox()).thenReturn(mockPlannerToolbox);
    when(mockPlannerContext.getPlannerConfig()).thenReturn(PlannerConfig.builder().build());
    when(mockPlannerContext.getJsonMapper()).thenReturn(JsonMapper.builder().build());
    when(mockPlannerContext.getAuthorizationResult()).thenReturn(AUTHORIZATION_RESULT);

    LogicalTableScan logicalTableScan = new LogicalTableScan(
        mockRelOptCluster,
        mockRelTraitSet,
        ImmutableList.of(),
        mockRelOptTable
    );
    druidQueryRelNode = DruidQueryRel.scanTable(
        logicalTableScan,
        mockRelOptTable,
        DRUID_TABLE,
        mockPlannerContext
    );
  }

  @Test
  public void testDruidQueryRel()
  {
    DruidQuery queryForExplaining = druidQueryRelNode.toDruidQueryForExplaining();
    DruidQuery query = druidQueryRelNode.toDruidQuery(true);

    // explain query should return a TableDataSource.
    Assert.assertEquals(TABLE, queryForExplaining.getDataSource());
    // query should return a RestrictedDataSource.
    Assert.assertEquals(RESTRICTED, query.getDataSource());
  }

  @Test
  public void testDruidJoinQueryRel()
  {
    // Arrange
    when(mockPlannerContext.getJoinAlgorithm()).thenReturn(JoinAlgorithm.SORT_MERGE);
    mockExpressionParser();
    LogicalJoin logicalJoin = LogicalJoin.create(
        druidQueryRelNode,
        druidQueryRelNode,
        ImmutableList.of(),
        ALWAYS_TRUE,
        ImmutableSet.of(),
        JoinRelType.INNER
    );
    DruidJoinQueryRel joinRel = DruidJoinQueryRel.create(logicalJoin, null, mockPlannerContext);

    // Act
    DruidQuery queryForExplaining = joinRel.toDruidQueryForExplaining();
    DruidQuery query = joinRel.toDruidQuery(false);

    // Assert
    Assert.assertEquals(DruidJoinQueryRel.DUMMY_DATA_SOURCE, queryForExplaining.getDataSource());
    JoinDataSource dataSource = (JoinDataSource) query.getDataSource();
    Assert.assertEquals(RESTRICTED, ((QueryDataSource) dataSource.getLeft()).getQuery().getDataSource());
    Assert.assertEquals(RESTRICTED, ((QueryDataSource) dataSource.getRight()).getQuery().getDataSource());
  }

  @Test
  public void testDruidUnionQueryRel()
  {
    // Arrange
    DruidUnion union = new DruidUnion(
        mockRelOptCluster,
        mockRelTraitSet,
        ImmutableList.of(),
        ImmutableList.of(druidQueryRelNode, druidQueryRelNode),
        true
    );
    DruidUnionDataSourceRel rel = DruidUnionDataSourceRel.create(union, ImmutableList.of("col1"), mockPlannerContext);

    // Act
    DruidQuery queryForExplaining = rel.toDruidQueryForExplaining();
    DruidQuery query = rel.toDruidQuery(false);

    // Assert
    Assert.assertEquals(DruidUnionDataSourceRel.DUMMY_DATA_SOURCE, queryForExplaining.getDataSource());
    Assert.assertEquals(new UnionDataSource(ImmutableList.of(RESTRICTED, RESTRICTED)), query.getDataSource());
  }

  @Test
  public void testDruidOuterQueryRel()
  {
    // Arrange
    PartialDruidQuery partialDruidQuerySpy = spy(druidQueryRelNode.getPartialDruidQuery());
    when(partialDruidQuerySpy.getTraitSet(any(), any())).thenReturn(mockRelTraitSet);
    when(mockRelTraitSet.containsIfApplicable(any())).thenReturn(true);
    PartialDruidQuery inputQuery = PartialDruidQuery.createOuterQuery(
        partialDruidQuerySpy,
        druidQueryRelNode.getPlannerContext()
    );
    DruidOuterQueryRel rel = DruidOuterQueryRel.create(druidQueryRelNode, inputQuery);

    // Act
    DruidQuery queryForExplaining = rel.toDruidQueryForExplaining();
    DruidQuery query = rel.toDruidQuery(false);

    // Assert
    Assert.assertEquals(DruidOuterQueryRel.DUMMY_DATA_SOURCE, queryForExplaining.getDataSource());
    Assert.assertEquals(RESTRICTED, ((QueryDataSource) query.getDataSource()).getQuery().getDataSource());
  }

  @Test
  public void testDruidCorrelateUnnestRel()
  {
    // Arrange
    mockExpressionParser();
    DruidUnnestRel right = mock(DruidUnnestRel.class);
    when(right.getRowType()).thenReturn(REC_TYPE);
    when(right.getInputRexNode()).thenReturn(ALWAYS_TRUE);

    Correlate correlateRel = LogicalCorrelate.create(
        druidQueryRelNode,
        right,
        ImmutableList.of(),
        mock(CorrelationId.class),
        ImmutableBitSet.of(0),
        JoinRelType.LEFT
    );
    DruidCorrelateUnnestRel rel = DruidCorrelateUnnestRel.create(correlateRel, mockPlannerContext);

    // Act
    DruidQuery queryForExplaining = rel.toDruidQueryForExplaining();
    DruidQuery query = rel.toDruidQuery(false);

    // Assert
    Assert.assertEquals(DruidCorrelateUnnestRel.DUMMY_DATA_SOURCE, queryForExplaining.getDataSource());
    Assert.assertEquals(RESTRICTED, ((UnnestDataSource) query.getDataSource()).getBase());
  }

  @Test
  public void testDruidUnnestRel()
  {
    DruidUnnestRel rel = DruidUnnestRel.create(mockRelOptCluster, mockRelTraitSet, ALWAYS_TRUE, mockPlannerContext);

    CannotBuildQueryException e1 = Assert.assertThrows(CannotBuildQueryException.class, rel::toDruidQueryForExplaining);
    CannotBuildQueryException e2 = Assert.assertThrows(CannotBuildQueryException.class, () -> rel.toDruidQuery(false));
    Assert.assertEquals("Cannot execute UNNEST directly", e1.getMessage());
    Assert.assertEquals("Cannot execute UNNEST directly", e2.getMessage());
  }

  @Test
  public void testDruidUnionRel()
  {
    DruidUnionRel rel = DruidUnionRel.create(mockPlannerContext, REC_TYPE, ImmutableList.of(druidQueryRelNode), 1000);

    UnsupportedOperationException e1 = Assert.assertThrows(
        UnsupportedOperationException.class,
        rel::toDruidQueryForExplaining
    );
    UnsupportedOperationException e2 = Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> rel.toDruidQuery(false)
    );
  }

  private void mockExpressionParser()
  {
    ExpressionParser parser = mock(ExpressionParser.class);
    when(parser.parse(any())).thenReturn(EXPR_6L);
    when(mockPlannerContext.getExpressionParser()).thenReturn(parser);
    when(mockPlannerContext.parseExpression(any())).thenReturn(EXPR_6L);
  }
}
