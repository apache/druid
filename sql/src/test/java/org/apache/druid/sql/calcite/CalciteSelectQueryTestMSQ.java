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
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

import java.util.Arrays;

public class CalciteSelectQueryTestMSQ extends BaseCalciteQueryTest
{


  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
  }

  @Override
  public SqlEngine createEngine(
      QueryLifecycleFactory qlf, ObjectMapper queryJsonMapper,
      Injector injector
  )
  {
    return super.createEngine(qlf, queryJsonMapper, injector);
  }

  @Test
  public void testOrderThenLimitThenFilter()
  {
    testQueryWithMSQ(
        "SELECT dim1 FROM "
            + "(SELECT __time, dim1 FROM druid.foo ORDER BY __time DESC LIMIT 4) "
            + "WHERE dim1 IN ('abc', 'def')",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    new QueryDataSource(
                        newScanQueryBuilder()
                            .dataSource(CalciteTests.DATASOURCE1)
                            .intervals(querySegmentSpec(Filtration.eternity()))
                            .columns(ImmutableList.of("__time", "dim1"))
                            .limit(4)
                            .order(ScanQuery.Order.DESCENDING)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .context(QUERY_CONTEXT_DEFAULT)
                            .build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("dim1"))
                .filters(in("dim1", Arrays.asList("abc", "def"), null))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }
}
