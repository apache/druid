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


package org.apache.druid.sql.calcite.schema;

import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SystemSchemaProviderTest
{
  /** The Broker authorizes system tables directly if either the deprecated SQL property or the auth property is set. */
  @ParameterizedTest(name = "sql = {0}, auth = {1}")
  @CsvSource({"false, false, false", "true, false, true", "false, true, true", "true, true, true"})
  public void testSystemTableAuthorizationCombinesProperties(
      final boolean sqlProperty,
      final boolean authProperty,
      final boolean expected
  )
  {
    final PlannerConfig plannerConfig = PlannerConfig.builder().authorizeSystemTablesDirectly(sqlProperty).build();
    final AuthConfig authConfig = AuthConfig.newBuilder().setAuthorizeSystemTablesDirectly(authProperty).build();

    Assertions.assertEquals(
        expected,
        SystemSchemaProvider.withSystemTableAuthorization(plannerConfig, authConfig).isAuthorizeSystemTablesDirectly()
    );
  }
}
