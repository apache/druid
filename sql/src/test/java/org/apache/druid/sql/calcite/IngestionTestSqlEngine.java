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
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngines;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.destination.IngestDestination;

import java.util.Map;

public class IngestionTestSqlEngine implements SqlEngine
{
  public static final IngestionTestSqlEngine INSTANCE = new IngestionTestSqlEngine();

  private IngestionTestSqlEngine()
  {
  }

  @Override
  public String name()
  {
    return "ingestion-test";
  }

  @Override
  public void validateContext(Map<String, Object> queryContext)
  {
    // No validation.
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
  public boolean featureAvailable(final EngineFeature feature, final PlannerContext plannerContext)
  {
    switch (feature) {
      case CAN_SELECT:
      case ALLOW_BINDABLE_PLAN:
      case TIMESERIES_QUERY:
      case TOPN_QUERY:
      case TIME_BOUNDARY_QUERY:
      case SCAN_NEEDS_SIGNATURE:
      case UNNEST:
        return false;
      case CAN_INSERT:
      case CAN_REPLACE:
      case READ_EXTERNAL_DATA:
      case WRITE_EXTERNAL_DATA:
      case SCAN_ORDER_BY_NON_TIME:
      case ALLOW_BROADCAST_RIGHTY_JOIN:
      case ALLOW_TOP_LEVEL_UNION_ALL:
        return true;
      default:
        throw SqlEngines.generateUnrecognizedFeatureException(IngestionTestSqlEngine.class.getSimpleName(), feature);
    }
  }

  @Override
  public QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryMaker buildQueryMakerForInsert(IngestDestination destination, RelRoot relRoot, PlannerContext plannerContext)
  {
    final RowSignature signature = RowSignatures.fromRelDataType(
        relRoot.validatedRowType.getFieldNames(),
        relRoot.validatedRowType
    );

    return new TestInsertQueryMaker(destination, signature);
  }
}
