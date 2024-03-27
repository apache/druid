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

package org.apache.druid.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.util.CacheTestHelperModule;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerFixture;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardPlannerComponentSupplier;

import java.io.File;

/**
 * Test-specific "fixture" to package up the "Calcite test" query infrastructure
 * into a form needed to run gRPC unit tests. gRPC requires that the statement factory
 * be available at class setup time. Normally, the Calcite tests build this object
 * per test.
 * <p>
 * This fixture uses default values, which should be fine for gRPC tests: we want to
 * test the gRPC mechanism, not the planner or query stack. If any customization is
 * needed, it can be done here with a custom component supplier, etc.
 */
public class QueryFrameworkFixture
{

  private final SqlTestFramework queryFramework;
  private final PlannerFixture plannerFixture;

  public QueryFrameworkFixture(final File temporaryFolder)
  {
    QueryComponentSupplier componentSupplier = new StandardComponentSupplier(
        temporaryFolder
    );
    SqlTestFramework.Builder builder = new SqlTestFramework.Builder(componentSupplier)
        .minTopNThreshold(TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD)
        .mergeBufferCount(0)
        .withOverrideModule(CacheTestHelperModule.ResultCacheMode.DISABLED.makeModule());
    queryFramework = builder.build();
    PlannerComponentSupplier plannerComponentSupplier = new StandardPlannerComponentSupplier();
    AuthConfig authConfig = new AuthConfig();
    plannerFixture = queryFramework.plannerFixture(
        plannerComponentSupplier,
        BaseCalciteQueryTest.PLANNER_CONFIG_DEFAULT,
        authConfig
    );
  }

  public SqlStatementFactory statementFactory()
  {
    return plannerFixture.statementFactory();
  }

  public ObjectMapper jsonMapper()
  {
    return queryFramework.queryJsonMapper();
  }

  public QueryLifecycleFactory getQueryLifecycleFactory()
  {
    return queryFramework.queryLifecycleFactory();
  }
}
