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

package org.apache.druid.msq.test;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.sql.calcite.CalciteNestedDataQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Runs {@link CalciteNestedDataQueryTest} but with MSQ engine
 */
@SqlTestFrameworkConfig.ComponentSupplier(CalciteNestedDataQueryMSQTest.NestedDataQueryMSQComponentSupplier.class)
public class CalciteNestedDataQueryMSQTest extends CalciteNestedDataQueryTest
{

  public static class NestedDataQueryMSQComponentSupplier extends AbstractMSQComponentSupplierDelegate
  {
    public NestedDataQueryMSQComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(new NestedComponentSupplier(tempFolderProducer));
    }
  }

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .addCustomRunner(new ExtractResultsFactory(() -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }

  @Override
  @Test
  public void testJoinOnNestedColumnThrows()
  {
    Assertions.assertThrows(ISE.class, () -> {
      testQuery(
          "SELECT * FROM druid.nested a INNER JOIN druid.nested b ON a.nester = b.nester",
          ImmutableList.of(),
          ImmutableList.of()
      );
    });
  }

}
