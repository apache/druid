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

package org.apache.druid.quidem;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.TestDruidModule;
import org.apache.druid.server.QuerySchedulerProvider;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;
import org.apache.druid.sql.calcite.util.CalciteTests;

public class TestSqlModule extends TestDruidModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(String.class)
        .annotatedWith(DruidSchemaName.class)
        .toInstance(CalciteTests.DRUID_SCHEMA_NAME);
    binder.bind(CalciteRulesManager.class).toInstance(new CalciteRulesManager(ImmutableSet.of()));
    binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);
    binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
    binder.bind(AuthenticatorMapper.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_MAPPER);
    binder.bind(Escalator.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_ESCALATOR);
    binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(false));
  }
}
