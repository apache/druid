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

import org.apache.druid.error.DruidException;
import org.apache.druid.sql.calcite.CalciteStrictInsertTest.StrictInsertComponentSupplier;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.CatalogResolver.NullCatalogResolver;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.junit.jupiter.api.Test;

/**
 * Test for the "strict" feature of the catalog which can restrict INSERT statements
 * to only work with existing datasources. The strict option is a config option which
 * we enable only for this one test.
 */
@SqlTestFramework.SqlTestFrameWorkModule(StrictInsertComponentSupplier.class)
public class CalciteStrictInsertTest extends CalciteIngestionDmlTest
{
  static class StrictInsertComponentSupplier extends IngestionDmlComponentSupplier
  {
    public StrictInsertComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public CatalogResolver createCatalogResolver()
    {
      return new NullCatalogResolver() {
        @Override
        public boolean ingestRequiresExistingTable()
        {
          return true;
        }
      };
    }
  }

  @Test
  public void testInsertIntoNewTable()
  {
    testIngestionQuery()
        .sql("INSERT INTO dst SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectValidationError(DruidException.class, "Cannot INSERT into [dst] because it does not exist")
        .verify();
  }

  @Test
  public void testInsertIntoExisting()
  {
    testIngestionQuery()
        .sql("INSERT INTO druid.numfoo SELECT * FROM foo PARTITIONED BY ALL TIME")
        .expectTarget("numfoo", FOO_TABLE_SIGNATURE)
        .expectResources(dataSourceRead("foo"), dataSourceWrite("numfoo"))
        .expectQuery(
            newScanQueryBuilder()
                .dataSource("foo")
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(PARTITIONED_BY_ALL_TIME_QUERY_CONTEXT)
                .build()
        )
        .verify();
  }
}
