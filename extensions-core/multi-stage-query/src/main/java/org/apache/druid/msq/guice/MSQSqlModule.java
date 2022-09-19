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

package org.apache.druid.msq.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.guice.annotations.MSQ;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.metadata.input.InputSourceModule;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.List;
import java.util.Properties;

/**
 * Module for providing the {@code EXTERN} operator.
 */
@LoadScope(roles = NodeRole.BROKER_JSON_NAME)
public class MSQSqlModule implements DruidModule
{
  @Inject
  Properties properties = null;

  @Override
  public List<? extends Module> getJacksonModules()
  {
    // We want this module to bring input sources along for the ride.
    return new InputSourceModule().getJacksonModules();
  }

  @Override
  public void configure(Binder binder)
  {
    // We want this module to bring InputSourceModule along for the ride.
    binder.install(new InputSourceModule());

    binder.bind(MSQTaskSqlEngine.class).in(LazySingleton.class);

    // Set up the EXTERN macro.
    SqlBindings.addOperatorConversion(binder, ExternalOperatorConversion.class);
  }


  @Provides
  @MSQ
  @LazySingleton
  public SqlStatementFactory makeMSQSqlStatementFactory(
      final MSQTaskSqlEngine engine,
      SqlToolbox toolbox
  )
  {
    return new SqlStatementFactory(toolbox.withEngine(engine));
  }
}
