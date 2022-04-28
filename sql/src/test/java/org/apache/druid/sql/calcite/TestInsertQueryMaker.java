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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.QueryFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;

/**
 * QueryMaker used by {@link CalciteInsertDmlTest}.
 */
public class TestInsertQueryMaker implements QueryMaker
{
  private final RelDataType resultType;
  private final String targetDataSource;
  private final RowSignature signature;

  public TestInsertQueryMaker(
      final RelDataTypeFactory typeFactory,
      final String targetDataSource,
      final RowSignature signature
  )
  {
    this.resultType = typeFactory.createStructType(
        ImmutableList.of(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.OTHER)
        ),
        ImmutableList.of("dataSource", "signature")
    );
    this.targetDataSource = targetDataSource;
    this.signature = signature;
  }

  @Override
  public boolean feature(final QueryFeature feature)
  {
    switch (feature) {
      // INSERT queries should stick to groupBy, scan.
      case CAN_RUN_TIMESERIES:
      case CAN_RUN_TOPN:
      case CAN_RUN_TIME_BOUNDARY:
        return false;

      // INSERT uses external data.
      case CAN_READ_EXTERNAL_DATA:
        return true;

      // INSERT uses Scan + ORDER BY.
      case SCAN_CAN_ORDER_BY_NON_TIME:
        return true;

      default:
        throw new IAE("Unrecognized feature: %s", feature);
    }
  }

  @Override
  public RelDataType getResultType()
  {
    return resultType;
  }

  @Override
  public Sequence<Object[]> runQuery(final DruidQuery druidQuery)
  {
    // Don't actually execute anything, but do record information that tests will check for.

    // 1) Add the query to Hook.QUERY_PLAN, so it gets picked up by QueryLogHook.
    Hook.QUERY_PLAN.run(druidQuery.getQuery());

    // 2) Return the dataSource and signature of the insert operation, so tests can confirm they are correct.
    return Sequences.simple(ImmutableList.of(new Object[]{targetDataSource, signature}));
  }
}
