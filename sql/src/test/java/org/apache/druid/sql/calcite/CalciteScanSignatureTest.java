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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.destination.IngestDestination;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CalciteScanSignatureTest extends BaseCalciteQueryTest
{
  public CalciteScanSignatureTest()
  {
    super(null);
  }

  @Test
  public void testScanSignature()
  {
    final Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(DruidQuery.CTX_SCAN_SIGNATURE, "[{\"name\":\"v0\",\"type\":\"STRING\"}]");

    testQuery(
        "SELECT CONCAT(dim1, '-', dim1, '_', dim1) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",'-',\"dim1\",'_',\"dim1\")",
                    ColumnType.STRING
                ))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"-_"},
            new Object[]{"10.1-10.1_10.1"},
            new Object[]{"2-2_2"},
            new Object[]{"1-1_1"},
            new Object[]{"def-def_def"},
            new Object[]{"abc-abc_abc"}
        )
    );
  }

  @Test
  public void testScanSignatureWithDimAsValuePrimitiveByteArr()
  {
    final Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    testQuery(
        "SELECT CAST(dim1 AS BIGINT) as dimX FROM foo2 limit 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE2)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("v0")
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "CAST(\"dim1\", 'LONG')",
                    ColumnType.LONG
                ))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(context)
                .limit(2)
                .build()
        ),
        useDefault ? ImmutableList.of(
            new Object[]{0L}, new Object[]{0L}
        ) : ImmutableList.of(
            new Object[]{null}, new Object[]{null}
        )
    );
  }

  @Override
  public SqlEngine createEngine(
      QueryLifecycleFactory qlf,
      ObjectMapper queryJsonMapper,
      Injector injector
  )
  {
    // Create an engine that says yes to EngineFeature.SCAN_NEEDS_SIGNATURE.
    return new ScanSignatureTestSqlEngine(super.createEngine(qlf, queryJsonMapper, injector));
  }

  private static class ScanSignatureTestSqlEngine implements SqlEngine
  {
    private final SqlEngine parent;

    public ScanSignatureTestSqlEngine(final SqlEngine parent)
    {
      this.parent = parent;
    }

    @Override
    public String name()
    {
      return getClass().getName();
    }

    @Override
    public boolean featureAvailable(EngineFeature feature, PlannerContext plannerContext)
    {
      return feature == EngineFeature.SCAN_NEEDS_SIGNATURE || parent.featureAvailable(feature, plannerContext);
    }

    @Override
    public void validateContext(Map<String, Object> queryContext)
    {
      // No validation.
    }

    @Override
    public RelDataType resultTypeForSelect(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
    {
      return validatedRowType;
    }

    @Override
    public RelDataType resultTypeForInsert(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext)
        throws ValidationException
    {
      return parent.buildQueryMakerForSelect(relRoot, plannerContext);
    }

    @Override
    public QueryMaker buildQueryMakerForInsert(IngestDestination destination, RelRoot relRoot, PlannerContext plannerContext)
    {
      throw new UnsupportedOperationException();
    }
  }
}
