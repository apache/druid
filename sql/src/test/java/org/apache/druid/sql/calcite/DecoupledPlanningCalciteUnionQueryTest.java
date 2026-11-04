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
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class DecoupledPlanningCalciteUnionQueryTest extends CalciteUnionQueryTest
{
  @RegisterExtension
  NotYetSupportedProcessor notYetSupportedProcessor = new NotYetSupportedProcessor(NotYetSupported.Scope.DECOUPLED);

  @RegisterExtension
  DecoupledExtension decoupledExtension = new DecoupledExtension(this);

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return decoupledExtension.testBuilder();
  }

  @Test
  public void testUnionAllLookups()
  {
    // Lookups cannot be arms of a UnionDataSource, so the union runs one query per arm. The coupled planner rejects
    // this query outright, so the case only arises here.
    skipVectorize();
    testBuilder()
        .sql(
            "SELECT COUNT(*) FROM (\n"
            + "SELECT k FROM lookup.lookyloo\n"
            + "UNION ALL\n"
            + "SELECT k FROM lookup.lookyloo\n"
            + ")"
        )
        .expectedResults(ImmutableList.of(new Object[]{8L}))
        .run();
  }
}
