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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.table.RowSignatures;

public class TestInsertSqlEngine implements SqlEngine
{
  public static final TestInsertSqlEngine INSTANCE = new TestInsertSqlEngine();

  private TestInsertSqlEngine()
  {
  }

  @Override
  public RelDataType resultTypeForSelect(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelDataType resultTypeForInsert(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
  {
    // Matches the return structure of TestInsertQueryMaker.
    return typeFactory.createStructType(
        ImmutableList.of(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.OTHER)
        ),
        ImmutableList.of("dataSource", "signature")
    );
  }

  @Override
  public boolean feature(final EngineFeature feature, final PlannerContext plannerContext)
  {
    switch (feature) {
      case CAN_RUN_TIMESERIES:
      case CAN_RUN_TOPN:
      case CAN_RUN_TIME_BOUNDARY:
      case ALLOW_BINDABLE_PLAN:
      case SCAN_NEEDS_SIGNATURE:
        return false;
      case CAN_INSERT:
      case CAN_READ_EXTERNAL_DATA:
      case SCAN_CAN_ORDER_BY_NON_TIME:
        return true;
      default:
        throw new IAE("Unrecognized feature: %s", feature);
    }
  }

  @Override
  public boolean isSystemContextParameter(String contextParameterName)
  {
    return NativeSqlEngine.SYSTEM_CONTEXT_PARAMETERS.contains(contextParameterName);
  }

  @Override
  public QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryMaker buildQueryMakerForInsert(String targetDataSource, RelRoot relRoot, PlannerContext plannerContext)
  {
    final RowSignature signature = RowSignatures.fromRelDataType(
        relRoot.validatedRowType.getFieldNames(),
        relRoot.validatedRowType
    );

    return new TestInsertQueryMaker(targetDataSource, signature);
  }
}
