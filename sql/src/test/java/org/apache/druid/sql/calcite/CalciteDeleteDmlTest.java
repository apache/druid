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

import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

public class CalciteDeleteDmlTest extends CalciteIngestionDmlTest
{
  @Test
  @Ignore
  public void testDeleteFromTableWithCondition()
  {
    testIngestionQuery()
        .sql("DELETE FROM foo WHERE dim1 = 'New' PARTITIONED BY ALL TIME")
        .expectTarget("foo", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(new NotDimFilter(new SelectorDimFilter("dim1", "New", null)))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();
  }

  @Test
  @Ignore
  public void testDeleteFromTableWithoutWhereClause()
  {
    testIngestionQuery()
        .sql("DELETE FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(
            SqlPlanningException.class,
            "WHERE clause must be present in DELETE statements. (To delete all rows, WHERE TRUE must be explicitly specified.)"
        )
        .verify();
  }

  @Test
  @Ignore
  public void testDeleteAllFromTable()
  {
    testIngestionQuery()
        .sql("DELETE FROM foo WHERE TRUE PARTITIONED BY ALL TIME")
        .expectTarget("foo", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("foo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource(InlineDataSource.fromIterable(Collections.emptyList(), FOO_TABLE_SIGNATURE))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(null)
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(REPLACE_ALL_TIME_CHUNKS)
                .build()
        )
        .verify();

  }
}
